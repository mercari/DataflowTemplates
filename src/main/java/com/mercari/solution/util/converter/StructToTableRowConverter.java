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
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to BigQuery TableRow object
 */
public class StructToTableRowConverter {

    private static final Logger LOG = LoggerFactory.getLogger(StructToTableRowConverter.class);

    private StructToTableRowConverter() {}

    /**
     * Convert Spanner {@link Struct} object to BigQuery {@link TableRow} object.
     *
     * @param struct Spanner Struct to be converted to TableRow object.
     * @return BigQuery TableRow object.
     */
    public static TableRow convert(final Struct struct) {
        TableRow row = new TableRow();
        for (final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            if ("f".equals(fieldName)) {
                throw new IllegalArgumentException("Struct must not have field name f because `f` is reserved tablerow field name.");
            }
            row = setFieldValue(row, fieldName, field.getType(), struct);
        }
        return row;
    }

    /**
     * Convert Spanner {@link Struct} to Json string of BigQuery {@link TableSchema} converted from input schema.
     * (for BigQueryIO.Write.withSchemaFromView)
     *
     * @param output Output string
     * @param struct Spanner Struct object
     * @return Map of output - json of schema.
     */
    public static Map<String,String> convertSchema(final String output, final Struct struct) {
        final List<TableFieldSchema> structFields = struct.getType().getStructFields().stream()
                .map(field -> getFieldTableSchema(field.getName(), field.getType(), struct))
                .collect(Collectors.toList());
        final String json = new Gson().toJson(new TableSchema().setFields(structFields));
        LOG.info(String.format("Spanner Query Result Schema Json: %s", json));
        final Map<String,String> map = new HashMap<>();
        map.put(output, json);
        return map;
    }

    private static TableRow setFieldValue(TableRow row, final String fieldName, final Type type,final Struct struct) {
        if(struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        switch (type.getCode()) {
            case STRING:
                return row.set(fieldName, struct.getString(fieldName));
            case BYTES:
                return row.set(fieldName, struct.getBytes(fieldName));
            case BOOL:
                return row.set(fieldName, struct.getBoolean(fieldName));
            case INT64:
                return row.set(fieldName, struct.getLong(fieldName));
            case FLOAT64:
                return row.set(fieldName, struct.getDouble(fieldName));
            case DATE:
                return row.set(fieldName, struct.getDate(fieldName));
            case TIMESTAMP:
                return row.set(fieldName, struct.getTimestamp(fieldName).getSeconds());
            case STRUCT:
                final Struct childStruct = struct.getStruct(fieldName);
                TableRow childRow = new TableRow();
                for(Type.StructField field : childStruct.getType().getStructFields()) {
                    childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                }
                return row.set(fieldName, childRow);
            case ARRAY:
                LOG.info(type.getArrayElementType().toString());
                return setArrayFieldValue(row, fieldName, type.getArrayElementType(), struct);
            default:
                return row;
        }
    }

    private static TableRow setArrayFieldValue(TableRow row, final String fieldName, final Type type, final Struct struct) {
        if (struct.isNull(fieldName)) {
            return row.set(fieldName, null);
        }
        switch (type.getCode()) {
            case STRING:
                return row.set(fieldName, struct.getStringList(fieldName));
            case BYTES:
                return row.set(fieldName, struct.getBytesList(fieldName));
            case BOOL:
                return row.set(fieldName, struct.getBooleanList(fieldName));
            case INT64:
                return row.set(fieldName, struct.getLongList(fieldName));
            case FLOAT64:
                return row.set(fieldName, struct.getDoubleList(fieldName));
            case DATE:
                return row.set(fieldName, struct.getDateList(fieldName));
            case TIMESTAMP:
                final List<Long> timestampList = struct.getTimestampList(fieldName).stream()
                        .map(timestamp -> timestamp.getSeconds())
                        .collect(Collectors.toList());
                return row.set(fieldName, timestampList);
            case STRUCT:
                final List<TableRow> childRows = new ArrayList<>();
                for(Struct childStruct : struct.getStructList(fieldName)) {
                    TableRow childRow = new TableRow();
                    for(final Type.StructField field : childStruct.getType().getStructFields()) {
                        childRow = setFieldValue(childRow, field.getName(), field.getType(), childStruct);
                    }
                    childRows.add(childRow);
                }
                return row.set(fieldName, childRows);
            case ARRAY:
                // Not support ARRAY in ARRAY
                return row;
            default:
                return row;
        }
    }

    private static TableFieldSchema getFieldTableSchema(final String fieldName, final Type type, final Struct struct) {
        switch (type.getCode()) {
            case STRING:
                return new TableFieldSchema().setName(fieldName).setType("STRING").setMode("NULLABLE");
            case BYTES:
                return new TableFieldSchema().setName(fieldName).setType("BYTES").setMode("NULLABLE");
            case BOOL:
                return new TableFieldSchema().setName(fieldName).setType("BOOL").setMode("NULLABLE");
            case INT64:
                return new TableFieldSchema().setName(fieldName).setType("INT64").setMode("NULLABLE");
            case FLOAT64:
                return new TableFieldSchema().setName(fieldName).setType("FLOAT64").setMode("NULLABLE");
            case DATE:
                return new TableFieldSchema().setName(fieldName).setType("DATE").setMode("NULLABLE");
            case TIMESTAMP:
                return new TableFieldSchema().setName(fieldName).setType("TIMESTAMP").setMode("NULLABLE");
            case STRUCT:
                final Struct childStruct = struct.getStruct(fieldName);
                final List<TableFieldSchema> childStructFields = childStruct.getType().getStructFields().stream()
                        .map(field -> getFieldTableSchema(field.getName(), field.getType(), childStruct))
                        .collect(Collectors.toList());
                return new TableFieldSchema().setName(fieldName).setType("STRUCT").setFields(childStructFields).setMode("NULLABLE");
            case ARRAY:
                return getFieldTableSchema(fieldName, type.getArrayElementType(), struct).setMode("REPEATED");
            default:
                throw new IllegalArgumentException("");
        }
    }

}