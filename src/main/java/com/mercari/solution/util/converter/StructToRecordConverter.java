/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to Avro GenericRecord
 */
public class StructToRecordConverter {

    private static final MutableDateTime EPOCH_DATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    private StructToRecordConverter() {}

    /**
     * Convert Spanner {@link Struct} object to Avro {@link GenericRecord} object.
     *
     * @param struct Spanner Struct to be converted to GenericRecord object.
     * @param schema Avro schema object.
     * @return Avro GenericRecord object.
     */
    public static GenericRecord convert(Struct struct, Schema schema) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            setFieldValue(builder, field, struct);
        }
        return builder.build();
    }

    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Struct struct) {
        setFieldValue(builder, field, field.schema(), struct);
    }

    private static void setFieldValue(GenericRecordBuilder builder, Schema.Field field, Schema schema, Struct struct) {
        final String fieldName = field.name();
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", fieldName));
        }

        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }

        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getString(fieldName));
                break;
            case BYTES:
                builder.set(field, struct.getBytes(fieldName).asReadOnlyByteBuffer());
                break;
            case INT:
                if(Type.date().equals(type.get())) {
                    final Date date = struct.getDate(fieldName);
                    final DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
                    final Days days = Days.daysBetween(EPOCH_DATETIME, datetime);
                    builder.set(field, days.getDays());
                } else {
                    builder.set(field, struct.getLong(fieldName));
                }
                break;
            case LONG:
                if(Type.timestamp().equals(type.get())) {
                    builder.set(field, struct.getTimestamp(fieldName).getSeconds() * 1000);
                } else {
                    builder.set(field, struct.getLong(fieldName));
                }
                break;
            case DOUBLE:
                builder.set(field, struct.getDouble(fieldName));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBoolean(fieldName));
                break;
            case RECORD:
                final GenericRecord chileRecord = convert(struct.getStruct(fieldName), schema);
                builder.set(field, chileRecord);
                break;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setFieldValue(builder, field, childSchema, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, field, schema.getElementType(), struct);
                break;
            case NULL:
                builder.set(fieldName, null);
                break;
            case FLOAT:
            case ENUM:
            case MAP:
            case FIXED:
                break;
            default:
                break;
        }
    }

    private static void setArrayFieldValue(GenericRecordBuilder builder, Schema.Field field, Schema schema, Struct struct) {
        final String fieldName = field.name();
        final Optional<Type> type = struct.getType().getStructFields().stream()
                .filter(f -> f.getName().equals(fieldName))
                .map(f -> f.getType())
                .findFirst();

        if(!type.isPresent()) {
            throw new IllegalArgumentException(String.format("Missing field %s", fieldName));
        }
        if(struct.isNull(fieldName)) {
            builder.set(fieldName, null);
            return;
        }
        switch (schema.getType()) {
            case STRING:
                builder.set(field, struct.getStringList(fieldName));
                break;
            case BYTES:
                builder.set(field, struct.getBytesList(fieldName)
                        .stream()
                        .map(b -> b.asReadOnlyByteBuffer())
                        .collect(Collectors.toList()));
                break;
            case INT:
                if(Type.array(Type.date()).equals(type.get())) {
                    builder.set(field, struct.getDateList(fieldName).stream()
                            .map(date -> new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC))
                            .map(datetime -> Days.daysBetween(EPOCH_DATETIME, datetime).getDays())
                            .collect(Collectors.toList()));
                } else {
                    builder.set(field, struct.getLongList(fieldName));
                }
                break;
            case LONG:
                if(Type.array(Type.timestamp()).equals(type.get())) {
                    builder.set(field, struct.getTimestampList(fieldName).stream()
                            .map(timestamp -> timestamp.getSeconds() * 1000)
                            .collect(Collectors.toList()));
                } else {
                    builder.set(field, struct.getLongList(fieldName));
                }
                break;
            case DOUBLE:
                builder.set(field, struct.getDoubleList(fieldName));
                break;
            case BOOLEAN:
                builder.set(field, struct.getBooleanList(fieldName));
                break;
            case RECORD:
                builder.set(field, struct.getStructList(fieldName).stream()
                        .map(childStruct -> convert(childStruct, schema))
                        .collect(Collectors.toList()));
                break;
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    setArrayFieldValue(builder, field, childSchema, struct);
                }
                break;
            case ARRAY:
                setArrayFieldValue(builder, field, schema.getElementType(), struct);
                break;
            case NULL:
                builder.set(fieldName, null);
                break;
            case FLOAT:
            case ENUM:
            case MAP:
            case FIXED:
                break;
            default:
                break;
        }
    }

}