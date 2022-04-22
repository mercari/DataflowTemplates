/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Converter converts Cloud Spanner Struct to Json text row
 */
public class StructToJsonConverter {

    private StructToJsonConverter() {}

    /**
     * Convert Cloud Spanner {@link Struct} object to Json row string.
     *
     * @param struct Cloud Spanner Struct object
     * @return Json row string.
     */
    public static String convert(final Struct struct) {
        JsonObject obj = new JsonObject();
        struct.getType().getStructFields().stream()
                .forEach(f -> setJsonFieldValue(obj, f, struct));
        return obj.toString();
    }

    private static void setJsonFieldValue(JsonObject obj, Type.StructField field, Struct struct) {
        final String fieldName = field.getName();
        final boolean isNullField = struct.isNull(fieldName);
        switch (field.getType().getCode()) {
            case BOOL:
                obj.addProperty(fieldName, isNullField ? null : struct.getBoolean(fieldName));
                break;
            case INT64:
                obj.addProperty(fieldName, isNullField ? null : struct.getLong(fieldName));
                break;
            case FLOAT64:
                obj.addProperty(fieldName, isNullField ? null : struct.getDouble(fieldName));
                break;
            case STRING:
                obj.addProperty(fieldName, isNullField ? null : struct.getString(fieldName));
                break;
            case NUMERIC:
                obj.addProperty(fieldName, isNullField ? null : struct.getBigDecimal(fieldName));
                break;
            case BYTES:
                obj.addProperty(fieldName, isNullField ? null : struct.getBytes(fieldName).toBase64());
                break;
            case TIMESTAMP:
                obj.addProperty(fieldName, isNullField ? null : struct.getTimestamp(fieldName).toString());
                break;
            case DATE:
                obj.addProperty(fieldName, isNullField ? null : struct.getDate(fieldName).toString());
                break;
            case STRUCT:
                if(isNullField) {
                    obj.add(field.getName(), null);
                    return;
                }
                Struct childStruct = struct.getStruct(fieldName);
                JsonObject childObj = new JsonObject();
                for(Type.StructField childField : childStruct.getType().getStructFields()) {
                    setJsonFieldValue(childObj, childField, childStruct);
                }
                obj.add(fieldName, childObj);
                break;
            case ARRAY:
                setJsonArrayFieldValue(obj, field, struct);
                break;
        }
    }

    private static void setJsonArrayFieldValue(JsonObject obj, Type.StructField field, Struct struct) {
        if(struct.isNull(field.getName())) {
            obj.add(field.getName(), null);
            return;
        }
        JsonArray array = new JsonArray();
        switch (field.getType().getArrayElementType().getCode()) {
            case BOOL:
                struct.getBooleanList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case INT64:
                struct.getLongList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case FLOAT64:
                struct.getDoubleList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case STRING:
                struct.getStringList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case NUMERIC:
                struct.getBigDecimalList(field.getName()).stream().forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case BYTES:
                struct.getBytesList(field.getName()).stream().map(ByteArray::toBase64).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case TIMESTAMP:
                struct.getTimestampList(field.getName()).stream().map(s -> s.toString()).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case DATE:
                struct.getDateList(field.getName()).stream().map((Date date) -> date.toString()).forEach(array::add);
                obj.add(field.getName(), array);
                break;
            case STRUCT:
                for(Struct childStruct : struct.getStructList(field.getName())) {
                    JsonObject childObj = new JsonObject();
                    for(Type.StructField childField : childStruct.getType().getStructFields()) {
                        setJsonFieldValue(childObj, childField, childStruct);
                    }
                    array.add(childObj);
                }
                obj.add(field.getName(), array);
                break;
            case ARRAY:
                setJsonArrayFieldValue(obj, field, struct);
                break;
        }
    }

}