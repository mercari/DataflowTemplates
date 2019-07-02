/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converter converts Avro GenericRecord to BigQuery TableRow
 */
public class RecordToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(RecordToTableRowConverter.class);

    private enum TableRowFieldType {
        STRING,
        BYTES,
        INT64,
        FLOAT64,
        NUMERIC,
        BOOL,
        DATE,
        TIME,
        DATETIME,
        TIMESTAMP,
        GEOGRAPHY,
        ARRAY,
        STRUCT
    }

    private enum TableRowFieldMode {
        REQUIRED,
        NULLABLE,
        REPEATED
    }

    /**
     * Convert Avro format object {@link GenericRecord} to BigQuery {@link TableRow}.
     *
     * @param record Avro GenericRecord object
     * @return BigQuery TableRow converted from GenericRecord.
     */
    public static TableRow convert(GenericRecord record) {
        final TableRow row = new TableRow();
        for(final Schema.Field field : record.getSchema().getFields()) {
            row.set(field.name(), convertTableRowValue(field.schema(), record.get(field.name())));
        }
        return row;
    }

    /**
     * Convert Avro format object {@link SchemaAndRecord} to BigQuery {@link TableRow}.
     *
     * @param schemaAndRecord Avro object from BigQuery export results that contains BigQuery schema info and GenericRecord
     * @return BigQuery TableRow converted from GenericRecord.
     */
    public static TableRow convert(SchemaAndRecord schemaAndRecord) {
        return convert(schemaAndRecord.getRecord());
    }

    /**
     * Convert Avro {@link Schema} to BigQuery {@link TableSchema}.
     *
     * @param schema Avro GenericRecord Schema
     * @return BigQuery TableSchema converted from Avro Schema.
     */
    public static TableSchema convertTableSchema(final Schema schema) {
        final List<TableFieldSchema> structFields = schema.getFields().stream()
                .map(field -> getFieldTableSchema(field.name(), field.schema()))
                .filter(fieldSchema -> fieldSchema != null)
                .collect(Collectors.toList());
        return new TableSchema().setFields(structFields);
    }

    /**
     * Convert Avro format object {@link GenericRecord} to BigQuery {@link TableSchema}.
     *
     * @param record Avro GenericRecord object
     * @return BigQuery TableSchema converted from Avro GenericRecord.
     */
    public static TableSchema convertTableSchema(final GenericRecord record) {
        return convertTableSchema(record.getSchema());
    }

    /**
     * Convert Avro {@link Schema} to Json string of BigQuery {@link TableSchema} converted from input schema.
     * (for BigQueryIO.Write.withSchemaFromView)
     *
     * @param output Output string
     * @param schema Avro GenericRecord Schema
     * @return Map of output - json of schema.
     */
    public static Map<String, String> convertTableSchema(final ValueProvider<String> output, final Schema schema) {
        final TableSchema tableSchema = convertTableSchema(schema);
        final String json = new Gson().toJson(tableSchema);
        LOG.info(String.format("Spanner Query Result Schema Json: %s", json));
        final Map<String,String> map = new HashMap<>();
        map.put(output.get(), json);
        return map;
    }

    /**
     * Convert Avro {@link GenericRecord} to Json string of BigQuery {@link TableSchema} converted from input schema.
     * (for BigQueryIO.Write.withSchemaFromView)
     *
     * @param output Output string
     * @param record Avro GenericRecord object
     * @return Map of output - json of schema.
     */
    public static Map<String, String> convertTableSchema(final ValueProvider<String> output, final GenericRecord record) {
        return convertTableSchema(output, record.getSchema());
    }


    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema) {
        return getFieldTableSchema(fieldName, schema, TableRowFieldMode.REQUIRED);
    }

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Schema schema, final TableRowFieldMode mode) {
        if(schema == null) {
            throw new IllegalArgumentException(String.format("Schema of field: %s must not be null!", fieldName));
        }
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                if(AvroSchemaUtil.isSqlTypeDatetime(schema)) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.DATETIME, mode);
                } else if(AvroSchemaUtil.isSqlTypeGeography(schema)) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.GEOGRAPHY, mode);
                }
                return buildTableFieldSchema(fieldName, TableRowFieldType.STRING, mode);
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.NUMERIC, mode);
                }
                return buildTableFieldSchema(fieldName, TableRowFieldType.BYTES, mode);
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.DATE, mode);
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.TIME, mode);
                }
                return buildTableFieldSchema(fieldName, TableRowFieldType.INT64, mode);
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.TIMESTAMP, mode);
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return buildTableFieldSchema(fieldName, TableRowFieldType.TIME, mode);
                }
                return buildTableFieldSchema(fieldName, TableRowFieldType.INT64, mode);
            case FLOAT:
            case DOUBLE:
                return buildTableFieldSchema(fieldName, TableRowFieldType.FLOAT64, mode);
            case BOOLEAN:
                return buildTableFieldSchema(fieldName, TableRowFieldType.BOOL, mode);
            case RECORD:
                final List<TableFieldSchema> structFieldSchemas = schema.getFields().stream()
                        .map(field -> getFieldTableSchema(field.name(), field.schema()))
                        .collect(Collectors.toList());
                return buildTableFieldSchema(fieldName, TableRowFieldType.STRUCT, mode, structFieldSchemas);
            case MAP:
                final List<TableFieldSchema> mapFieldSchemas = ImmutableList.of(
                        new TableFieldSchema().setName("key").setType(TableRowFieldType.STRING.name()).setMode(TableRowFieldMode.REQUIRED.name()),
                        getFieldTableSchema("value", schema.getValueType()));
                return buildTableFieldSchema(fieldName, TableRowFieldType.STRUCT, TableRowFieldMode.REPEATED, mapFieldSchemas);
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                        .findAny().orElse(null);
                return getFieldTableSchema(fieldName, childSchema, TableRowFieldMode.NULLABLE);
            case ARRAY:
                return getFieldTableSchema(fieldName, schema.getElementType()).setMode(TableRowFieldMode.REPEATED.name());
            case NULL:
                // BigQuery ignores NULL value
                // https://cloud.google.com/bigquery/data-formats#avro_format
                throw new IllegalArgumentException();
            default:
                throw new IllegalArgumentException();
        }
    }

    private static Object convertTableRowValue(final Schema schema, final Object value) {
        return convertTableRowValue(schema, value, false);
    }

    private static Object convertTableRowValues(final Schema schema, final Object value) {
        return convertTableRowValue(schema, value, true);
    }

    private static Object convertTableRowValue(final Schema schema, final Object value, final boolean isArray) {
        if(schema == null) {
            throw new IllegalArgumentException(String.format("Schema of fieldValue: %v must not be null!", value));
        }
        if(value == null) {
            return null;
        }
        if(isArray) {
            return ((List<Object>)value).stream()
                    .map(v -> convertTableRowValue(schema, v))
                    .filter(v -> v != null)
                    .collect(Collectors.toList());
        }
        switch(schema.getType()) {
            case ENUM:
            case STRING:
                return value.toString();
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final byte[] bytes;
                    if(Schema.Type.FIXED.equals(schema.getType())) {
                        bytes = ((GenericData.Fixed)value).bytes();
                    } else {
                        bytes = ((ByteBuffer)value).array();
                    }
                    if(bytes.length == 0) {
                        return BigDecimal.valueOf(0, 0);
                    }
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    return BigDecimal.valueOf(new BigInteger(bytes).longValue(), scale);
                }
                if(Schema.Type.FIXED.equals(schema.getType())) {
                    return ByteBuffer.wrap(((GenericData.Fixed)value).bytes());
                }
                return value;
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay((Integer)value);
                    return localDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    final Long intValue = new Long((Integer)value);
                    final LocalTime localTime = LocalTime.ofNanoOfDay(intValue * 1000 * 1000);
                    return localTime.format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return value;
            case LONG:
                final Long longValue = (Long)value;
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return longValue / 1000;
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return longValue / 1000000;
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    LocalTime time = LocalTime.ofNanoOfDay(longValue * 1000);
                    return time.format(DateTimeFormatter.ISO_LOCAL_TIME);
                }
                return value;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return value;
            case RECORD:
                return convert((GenericRecord) value);
            case MAP:
                final Map<Object, Object> map = (Map)value;
                return map.entrySet().stream()
                        .map(entry -> new TableRow()
                                .set("key", entry.getKey() == null ? "" : entry.getKey().toString())
                                .set("value", convertTableRowValue(schema.getValueType(), entry.getValue())))
                        .collect(Collectors.toList());
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !Schema.Type.NULL.equals(s.getType()))
                        .findAny().orElse(null);
                return convertTableRowValue(childSchema, value);
            case ARRAY:
                return convertTableRowValues(schema.getElementType(), value);
            default:
                return value;
        }
    }

    private static TableFieldSchema buildTableFieldSchema(final String fieldName, TableRowFieldType type, TableRowFieldMode mode) {
        return new TableFieldSchema()
                .setName(fieldName)
                .setType(type.name())
                .setMode(mode.name());
    }

    private static TableFieldSchema buildTableFieldSchema(final String fieldName, TableRowFieldType type, TableRowFieldMode mode, List<TableFieldSchema> fieldSchemas) {
        return new TableFieldSchema()
                .setName(fieldName)
                .setType(type.name())
                .setFields(fieldSchemas)
                .setMode(mode.name());
    }

}