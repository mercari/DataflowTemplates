/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converter converts Avro GenericRecord to Cloud Spanner Struct
 */
public class RecordToStructConverter {

    private RecordToStructConverter() {}

    /**
     * Convert Avro format object {@link GenericRecord} to Cloud Spanner {@link Struct}.
     *
     * @param record Avro GenericRecord object
     * @return Cloud Spanner Struct converted from GenericRecord.
     */
    public static Struct convert(final GenericRecord record) {
        return convert(record.getSchema(), record);
    }

    /**
     * Convert Avro format object {@link GenericRecord} to Cloud Spanner {@link Struct}.
     *
     * @param schema Avro Schema object
     * @param record Avro GenericRecord object
     * @return Cloud Spanner Struct converted from GenericRecord.
     */
    public static Struct convert(final Schema schema, final GenericRecord record) {
        Struct.Builder builder = Struct.newBuilder();
        for(final Schema.Field field : schema.getFields()) {
            builder = setFieldValue(builder, field.name(), field.schema(), record);
        }
        return builder.build();
    }

    /**
     * Convert Avro format object {@link SchemaAndRecord} to Cloud Spanner {@link Struct}.
     *
     * @param schemaAndRecord Avro object from BigQuery export results that contains BigQuery schema info and GenericRecord
     * @return Cloud Spanner Struct converted from GenericRecord.
     */
    public static Struct convert(final SchemaAndRecord schemaAndRecord) {
        if(schemaAndRecord.getTableSchema() == null) {
            return convert(schemaAndRecord.getRecord().getSchema(), schemaAndRecord.getRecord());
        }
        final Schema schema = AvroSchemaUtil.convertSchema(schemaAndRecord.getTableSchema());
        return convert(schema, schemaAndRecord.getRecord());
    }

    private static Struct.Builder setFieldValue(final Struct.Builder builder, final String fieldName, final Schema schema, final GenericRecord record) {
        final Object value = record.get(fieldName);
        final boolean isNullField = value == null;
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return builder.set(fieldName).to(isNullField ? null : value.toString());
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    if(isNullField) {
                        return builder.set(fieldName).to((String)null);
                    }
                    byte[] bytes;
                    if(Schema.Type.FIXED.equals(schema.getType())) {
                        bytes = ((GenericData.Fixed)value).bytes();
                    } else {
                        bytes = ((ByteBuffer)value).array();
                    }
                    if(bytes.length == 0) {
                        bytes = BigDecimal.valueOf(0, 0).toBigInteger().toByteArray();
                    }
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    final String strValue = isNullField ? null : convertNumericBytesToString(bytes, scale);
                    return builder.set(fieldName).to(strValue);
                }
                return builder.set(fieldName).to(isNullField ? null : ByteArray.copyFrom((ByteBuffer)value));
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).to(isNullField ? null : convertEpochDaysToDate((Integer)value));
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).to(isNullField ? null :
                            LocalTime.ofNanoOfDay(new Long((Integer) value) * 1000 * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME));
                }
                return builder.set(fieldName).to(isNullField ? null : new Long((Integer) value));
            case LONG:
                final Long longValue = (Long)value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).to(isNullField ? null : convertMicrosecToTimestamp(longValue));
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).to(isNullField ? null : convertMicrosecToTimestamp(longValue));
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).to(isNullField ? null : convertNanosecToTimeString(longValue * 1000));
                }
                return builder.set(fieldName).to(isNullField ? null : longValue);
            case FLOAT:
                return builder.set(fieldName).to(isNullField ? null : new Double((Float) value));
            case DOUBLE:
                return builder.set(fieldName).to(isNullField ? null : (Double) value);
            case BOOLEAN:
                return builder.set(fieldName).to(isNullField ? null : (Boolean) value);
            case RECORD:
                final Struct childStruct = convert((GenericRecord) value);
                return builder.set(fieldName).to(isNullField ? null : childStruct);
            case MAP:
                return builder;
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return setFieldValue(builder, fieldName, childSchema, record);
            case ARRAY:
                return isNullField ? builder : setArrayFieldValue(builder, fieldName, schema.getElementType(), record);
            case NULL:
                return builder;
            default:
                return builder;
        }
    }

    private static Struct.Builder setArrayFieldValue(final Struct.Builder builder, final String fieldName, final Schema schema, final GenericRecord record) {
        final Object value = record.get(fieldName);
        final boolean isNullField = value == null;
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return builder.set(fieldName).toStringArray(isNullField ? null :
                        ((List<Object>) value).stream()
                                .map(utf8 -> utf8 == null ? null : utf8.toString())
                                .collect(Collectors.toList()));
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    if(Schema.Type.BYTES.equals(schema.getType())) {
                        return builder.set(fieldName).toStringArray(isNullField ? null :
                                ((List<ByteBuffer>) value).stream()
                                        .map(bytes -> convertNumericBytesToString(bytes.array(), scale))
                                        .collect(Collectors.toList()));
                    } else {
                        return builder.set(fieldName).toStringArray(isNullField ? null :
                                ((List<GenericData.Fixed>) value).stream()
                                        .map(bytes -> convertNumericBytesToString(bytes.bytes(), scale))
                                        .collect(Collectors.toList()));
                    }
                }
                return builder.set(fieldName).toBytesArray(isNullField ? null :
                        ((List<ByteBuffer>) value).stream()
                                .map(bytes -> bytes == null ? null : ByteArray.copyFrom(bytes))
                                .collect(Collectors.toList()));
            case INT:
                final List<Integer> intValues =  ((List<Integer>) value);
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).toDateArray(isNullField ? null : intValues.stream()
                            .map(days -> convertEpochDaysToDate(days))
                            .collect(Collectors.toList()));
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).toStringArray(isNullField ? null : intValues.stream()
                            .map(millis -> convertNanosecToTimeString(millis * 1000L * 1000L))
                            .collect(Collectors.toList()));
                }
                return builder.set(fieldName).toInt64Array(isNullField ? null : intValues.stream()
                        .map(i -> i == null ? null : new Long(i))
                        .collect(Collectors.toList()));
            case LONG:
                final List<Long> longValues = (List<Long>) value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).toTimestampArray(isNullField ? null : longValues.stream()
                            .map(millis -> convertMicrosecToTimestamp(millis * 1000))
                            .collect(Collectors.toList()));
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).toTimestampArray(isNullField ? null : longValues.stream()
                            .map(micros -> convertMicrosecToTimestamp(micros))
                            .collect(Collectors.toList()));
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return builder.set(fieldName).toStringArray(isNullField ? null : longValues.stream()
                            .map(micros -> convertNanosecToTimeString(micros * 1000))
                            .collect(Collectors.toList()));
                }
                return builder.set(fieldName).toInt64Array(longValues);
            case FLOAT:
                return builder.set(fieldName).toFloat64Array(isNullField ? null :
                        ((List<Float>) value).stream()
                                .map(f -> f == null ? null : new Double(f))
                                .collect(Collectors.toList()));
            case DOUBLE:
                return builder.set(fieldName).toFloat64Array((List<Double>) value);
            case BOOLEAN:
                return builder.set(fieldName).toBoolArray((List<Boolean>) value);
            case RECORD:
                // Currently, Not support conversion from nested avro record. (Only consider avro file by SpannerToAvro)
                return builder;
            case MAP:
                return builder;
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return setArrayFieldValue(builder, fieldName, childSchema, record);
            case ARRAY:
                // Currently, Not support conversion from nested array record. (Only consider avro file by SpannerToAvro)
                return builder;
            case NULL:
                return builder;
            default:
                return builder;
        }
    }

    private static Date convertEpochDaysToDate(final Integer epochDays) {
        if(epochDays == null) {
            return null;
        }
        final LocalDate ld = LocalDate.ofEpochDay(epochDays);
        return Date.fromYearMonthDay(ld.getYear(), ld.getMonth().getValue(), ld.getDayOfMonth());
    }

    private static String convertNanosecToTimeString(final Long nanos) {
        if(nanos == null) {
            return null;
        }
        final LocalTime localTime = LocalTime.ofNanoOfDay(nanos);
        return localTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
    }

    private static Timestamp convertMicrosecToTimestamp(final Long micros) {
        if(micros == null) {
            return null;
        }
        return Timestamp.ofTimeMicroseconds(micros);
    }

    private static String convertNumericBytesToString(final byte[] bytes, final int scale) {
        if(bytes == null) {
            return null;
        }
        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
        if(scale == 0) {
            return bigDecimal.toPlainString();
        }
        final StringBuilder sb = new StringBuilder(bigDecimal.toPlainString());
        while(sb.lastIndexOf("0") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        if(sb.lastIndexOf(".") == sb.length() - 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }
}