/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.*;
import com.google.cloud.spanner.Struct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility functions to get field value from struct object
 */
public class StructUtil {

    private StructUtil() {}

    /**
     * Get field value from Spanner {@link Struct} object.
     *
     * @param fieldName field name to get value.
     * @param struct Spanner Struct object.
     * @return Field value.
     */
    public static Object getFieldValue(final String fieldName, final Struct struct) {
        return struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .findAny()
                .map(f -> getFieldValue(f, struct))
                .orElse(null);
    }

    /**
     * Get field value from Spanner {@link Struct} object.
     *
     * @param field StructField to get value.
     * @param struct Spanner Struct object.
     * @return Field value.
     */
    public static Object getFieldValue(Type.StructField field, Struct struct) {
        if(struct.isNull(field.getName())) {
            return null;
        }
        switch (field.getType().getCode()) {
            case BOOL:
                return struct.getBoolean(field.getName());
            case INT64:
                return struct.getLong(field.getName());
            case FLOAT64:
                return struct.getDouble(field.getName());
            case STRING:
                return struct.getString(field.getName());
            case BYTES:
                return struct.getBytes(field.getName()).toBase64();
            case TIMESTAMP:
                return struct.getTimestamp(field.getName());
            case DATE:
                return struct.getDate(field.getName());
            case STRUCT:
                Map<String,Object> map = new HashMap<>();
                Struct childStruct = struct.getStruct(field.getName());
                for (Type.StructField childField : childStruct.getType().getStructFields()) {
                    map.put(childField.getName(), getFieldValue(childField, childStruct));
                }
                return map;
            case ARRAY:
                return getArrayFieldValue(field, struct);
        }
        return null;
    }

    private static Object getArrayFieldValue(Type.StructField field, Struct struct) {
        List list = new ArrayList<>();
        switch (field.getType().getArrayElementType().getCode()) {
            case BOOL:
                struct.getBooleanList(field.getName()).stream().forEach(list::add);
                return list;
            case INT64:
                struct.getLongList(field.getName()).stream().forEach(list::add);
                return list;
            case FLOAT64:
                struct.getDoubleList(field.getName()).stream().forEach(list::add);
                return list;
            case STRING:
                struct.getStringList(field.getName()).stream().forEach(list::add);
                return list;
            case BYTES:
                struct.getBytesList(field.getName()).stream().map((ByteArray::toBase64)).forEach(list::add);
                return list;
            case TIMESTAMP:
                struct.getTimestampList(field.getName()).stream().forEach(list::add);
                return list;
            case DATE:
                struct.getDateList(field.getName()).stream().forEach(list::add);
                return list;
            case STRUCT:
                List<Map<String,Object>> maps = new ArrayList<>();
                for (Struct childStruct : struct.getStructList(field.getName())) {
                    Map<String,Object> map = new HashMap<>();
                    for (Type.StructField childField : childStruct.getType().getStructFields()) {
                        map.put(childField.getName(), getFieldValue(childField, childStruct));
                    }
                    maps.add(map);
                }
                return maps;
            case ARRAY:
                return getArrayFieldValue(field, struct);
        }
        return null;
    }

}