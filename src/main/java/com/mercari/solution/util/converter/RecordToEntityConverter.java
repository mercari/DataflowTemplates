/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.datastore.v1.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;
import com.google.protobuf.util.Timestamps;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.ValueProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Converter converts Avro GenericRecord to Cloud Datastore Entity
 */
public class RecordToEntityConverter {

    private static final int MAX_STRING_SIZE_BYTES = 1500;
    private static String kind = null;
    private static String keyField = null;
    private static Set<String> excludeFromIndexFields = null;

    private static final Logger LOG = LoggerFactory.getLogger(RecordToEntityConverter.class);

    /**
     * Convert Avro format object {@link SchemaAndRecord} to Cloud Datastore {@link Entity}.
     *
     * @param record Avro object from BigQuery export results that contains BigQuery schema info and GenericRecord
     * @param kindVP Template option parameter specifies Datastore kind name to store.
     * @param keyFieldVP Template option parameter specifies field name to be key field.
     * @param excludeFromIndexFieldsVP Template option parameter specifies field name to be key field.
     * @return Cloud Datastore Entity converted from GenericRecord.
     */
    public static Entity convert(final SchemaAndRecord record,
                                 final ValueProvider<String> kindVP,
                                 final ValueProvider<String> keyFieldVP,
                                 final ValueProvider<String> excludeFromIndexFieldsVP) {
        if(record.getTableSchema() == null) {
            return convert(record.getRecord().getSchema(), record.getRecord(), kindVP, keyFieldVP, excludeFromIndexFieldsVP);
        } else {
            final Schema schema = AvroSchemaUtil.convertSchema(record.getTableSchema());
            return convert(schema, record.getRecord(), kindVP, keyFieldVP, excludeFromIndexFieldsVP);
        }
    }

    /**
     * Convert Avro format object {@link GenericRecord} to Cloud Datastore {@link Entity}.
     *
     * @param record Avro GenericRecord to convert to Entity
     * @param kindVP Template option parameter specifies Datastore kind name to store.
     * @param keyFieldVP Template option parameter specifies field name to be key field.
     * @param excludeFromIndexFieldsVP Template option parameter specifies field name to be key field.
     * @return Cloud Datastore Entity converted from GenericRecord.
     */
    public static Entity convert(final GenericRecord record,
                                 final ValueProvider<String> kindVP,
                                 final ValueProvider<String> keyFieldVP,
                                 final ValueProvider<String> excludeFromIndexFieldsVP) {
        return convert(record.getSchema(), record, kindVP, keyFieldVP, excludeFromIndexFieldsVP);
    }

    /**
     * Convert Avro format object {@link GenericRecord} and schema {@link Schema} to Cloud Datastore {@link Entity}.
     *
     * @param schema Avro Schema
     * @param record Avro GenericRecord to convert to Entity
     * @param kindVP Template option parameter specifies Datastore kind name to store.
     * @param keyFieldVP Template option parameter specifies field name to be key field.
     * @param excludeFromIndexFieldsVP Template option parameter specifies field name to be key field.
     * @return Cloud Datastore Entity converted from GenericRecord.
     */
    public static Entity convert(final Schema schema,
                                 final GenericRecord record,
                                 final ValueProvider<String> kindVP,
                                 final ValueProvider<String> keyFieldVP,
                                 final ValueProvider<String> excludeFromIndexFieldsVP) {
        if(kind == null) {
            kind = kindVP.get();
        }
        if(keyField == null) {
            keyField = keyFieldVP.get();
        }
        if(excludeFromIndexFields == null) {
            if(excludeFromIndexFieldsVP.get() == null) {
                excludeFromIndexFields = new HashSet<>();
            } else {
                excludeFromIndexFields = Arrays
                        .stream(excludeFromIndexFieldsVP.get().split(","))
                        .collect(Collectors.toSet());
            }
        }
        return convert(schema, record, kind, keyField, excludeFromIndexFields);
    }

    private static Entity convert(final Schema schema, final GenericRecord record,
                                  final String kind,
                                  final String keyField,
                                  final Set<String> excludeFromIndexFields) {
        Object keyValue = null;
        Schema.Type keyType = null;
        final Entity.Builder builder = Entity.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            final Object value = record.get(field.name());
            final Value.Builder valueBuilder = convertEntityValue(field.schema(), value);
            if(getType(field.schema()).equals(Schema.Type.ARRAY)) {
                builder.putProperties(field.name(), valueBuilder.build());
            } else if(excludeFromIndexFields.contains(field.name()) ||
                    (field.schema().getType().equals(Schema.Type.STRING) &&
                            value != null && value.toString().getBytes().length > MAX_STRING_SIZE_BYTES)) {
                builder.putProperties(field.name(), valueBuilder.setExcludeFromIndexes(true).build());
            } else {
                builder.putProperties(field.name(), valueBuilder.build());
            }
            if(field.name().equals(keyField)) {
                keyType = getType(field.schema());
                keyValue = value;
            }
        }

        if(keyType == null || keyValue == null) {
            throw new IllegalArgumentException("keyType and keyValue must not be null !");
        }

        return builder.setKey(Key.newBuilder().addPath(buildPathElement(keyType, keyValue, kind))).build();
    }

    private static Value.Builder convertEntityValue(final Schema schema, final Object value) {
        if(value == null) {
            return Value.newBuilder().setNullValue(NullValue.NULL_VALUE);
        }
        Value.Builder builder = Value.newBuilder();
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return builder.setStringValue(value.toString());
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final LogicalTypes.Decimal decimal = AvroSchemaUtil.getLogicalTypeDecimal(schema);
                    final byte[] bytes;
                    if(Schema.Type.FIXED.equals(schema.getType())) {
                        bytes = ((GenericData.Fixed)value).bytes();
                    } else {
                        bytes = ((ByteBuffer)value).array();
                    }
                    final String decimalValue = convertNumericBytesToString(bytes, decimal.getScale());
                    return builder.setStringValue(decimalValue);
                }
                if(Schema.Type.FIXED.equals(schema.getType())) {
                    return builder.setBlobValue(ByteString.copyFrom(((GenericData.Fixed)value).bytes()));
                } else {
                    return builder.setBlobValue(ByteString.copyFrom((ByteBuffer)value));
                }
            case INT:
                final Long intValue = new Long((Integer)value);
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay(intValue);
                    return builder.setStringValue(localDate.format(DateTimeFormatter.ISO_LOCAL_DATE));
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final LocalTime localTime = LocalTime.ofNanoOfDay(intValue * 1000 * 1000);
                    return builder.setStringValue(localTime.format(DateTimeFormatter.ISO_LOCAL_TIME));
                }
                return builder.setIntegerValue(intValue);
            case LONG:
                final Long longValue = (Long)value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return builder.setTimestampValue(Timestamps.fromMicros(longValue * 1000));
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return builder.setTimestampValue(Timestamps.fromMicros(longValue));
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    final LocalTime localTime = LocalTime.ofNanoOfDay(longValue * 1000);
                    return builder.setStringValue(localTime.format(DateTimeFormatter.ISO_LOCAL_TIME));
                }
                return builder.setIntegerValue(longValue);
            case FLOAT:
                return builder.setDoubleValue((Float)value);
            case DOUBLE:
                return builder.setDoubleValue((Double)value);
            case BOOLEAN:
                return builder.setBooleanValue((Boolean)value);
            case RECORD:
                Entity.Builder entityBuilder = Entity.newBuilder();
                final GenericRecord record = (GenericRecord)value;
                for(Schema.Field f : schema.getFields()) {
                    entityBuilder = entityBuilder.putProperties(f.name(), convertEntityValue(f.schema(), record.get(f.name())).build());
                }
                return builder.setEntityValue(entityBuilder.build());
            case ARRAY:
                final List<Value> arrayValues = ((List<Object>)value).stream()
                        .map(v -> convertEntityValue(schema.getElementType(), v).build())
                        .collect(Collectors.toList());
                final ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(arrayValues).build();
                return builder.setArrayValue(arrayValue);
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return convertEntityValue(childSchema, value);
            case MAP:
                final Map<Object, Object> map = (Map)value;
                final List<Value> mapValues = map.entrySet().stream()
                        .map(entry ->
                                Value.newBuilder().setEntityValue(
                                        Entity.newBuilder()
                                                .putProperties("key", Value.newBuilder().setStringValue(entry.getKey().toString()).build())
                                                .putProperties("value", convertEntityValue(schema.getValueType(), entry.getValue()).build())
                                                .build()
                                ).build())
                        .collect(Collectors.toList());
                final ArrayValue mapEntityValues = ArrayValue.newBuilder().addAllValues(mapValues).build();
                return builder.setArrayValue(mapEntityValues);
            case NULL:
            default:
                return builder.setNullValue(NullValue.NULL_VALUE);
        }
    }

    private static Key.PathElement buildPathElement(final Schema.Type keyType, final Object keyValue, final String kind) {
        final Key.PathElement.Builder pathBuilder = Key.PathElement.newBuilder().setKind(kind);
        switch (keyType) {
            case ENUM:
            case STRING:
                return pathBuilder.setName(keyValue.toString()).build();
            case BYTES:
                return pathBuilder.setNameBytes(ByteString.copyFrom((ByteBuffer)keyValue)).build();
            case INT:
                return pathBuilder.setId((Integer)keyValue).build();
            case LONG:
                return pathBuilder.setId((Long)keyValue).build();
            default:
                final String errorMessage = String.format("Key field must be STRING or INTEGER or LONG! but %s", keyType.getName());
                throw new IllegalArgumentException(errorMessage);
        }
    }

    private static Schema.Type getType(final Schema schema) {
        switch (schema.getType()) {
            case UNION:
                Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return getType(childSchema);
            default:
                return schema.getType();
        }
    }

    private static String convertNumericBytesToString(byte[] bytes, int scale) {
        if(bytes.length == 0) {
            return "";
        }
        BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

}