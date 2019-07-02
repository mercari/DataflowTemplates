/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;

/**
 * Converter converts Cloud Spanner Struct to Cloud Spanner Mutation
 */
public class StructToMutationConverter {

    private StructToMutationConverter() {}

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object.
     *
     * @param struct Spanner Struct to be converted to Mutation object.
     * @param table Spanner table name to store.
     * @param mutationOp Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE.
     * @return Spanner Mutation object.
     */
    public static Mutation convert(final Struct struct, final String table, final Mutation.Op mutationOp) {
        Mutation.WriteBuilder builder = createMutationWriteBuilder(table, mutationOp);
        for(final Type.StructField field : struct.getType().getStructFields()) {
            final String fieldName = field.getName();
            final boolean isNullField = struct.isNull(fieldName);
            switch(field.getType().getCode()) {
                case STRING:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getString(fieldName));
                    break;
                case BYTES:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getBytes(fieldName));
                    break;
                case BOOL:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getBoolean(fieldName));
                    break;
                case INT64:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getLong(fieldName));
                    break;
                case FLOAT64:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getDouble(fieldName));
                    break;
                case DATE:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getDate(fieldName));
                    break;
                case TIMESTAMP:
                    builder = builder.set(fieldName).to(isNullField ? null : struct.getTimestamp(fieldName));
                    break;
                case STRUCT:
                    // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                    // https://cloud.google.com/spanner/docs/data-types
                    break;
                case ARRAY:
                    switch (field.getType().getArrayElementType().getCode()) {
                        case STRING:
                            builder = builder.set(fieldName).toStringArray(isNullField ? null : struct.getStringList(fieldName));
                            break;
                        case BYTES:
                            builder = builder.set(fieldName).toBytesArray(isNullField ? null : struct.getBytesList(fieldName));
                            break;
                        case BOOL:
                            builder = builder.set(fieldName).toBoolArray(isNullField ? null : struct.getBooleanArray(fieldName));
                            break;
                        case INT64:
                            builder = builder.set(fieldName).toInt64Array(isNullField ? null : struct.getLongArray(fieldName));
                            break;
                        case FLOAT64:
                            builder = builder.set(fieldName).toFloat64Array(isNullField ? null : struct.getDoubleArray(fieldName));
                            break;
                        case DATE:
                            builder = builder.set(fieldName).toDateArray(isNullField ? null : struct.getDateList(fieldName));
                            break;
                        case TIMESTAMP:
                            builder = builder.set(fieldName).toTimestampArray(isNullField ? null : struct.getTimestampList(fieldName));
                            break;
                        case STRUCT:
                            // NOT SUPPOERTED TO STORE STRUCT AS FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            break;
                        case ARRAY:
                            // NOT SUPPOERTED TO STORE ARRAY IN ARRAY FIELD! (2019/03/04)
                            // https://cloud.google.com/spanner/docs/data-types
                            break;
                    }

            }
        }
        return builder.build();
    }

    /**
     * Convert Spanner {@link Struct} object to Spanner {@link Mutation} object to delete.
     *
     * @param struct Spanner Struct to be deleted.
     * @param table Spanner table name to delete specified struct object.
     * @param keyFields Key fields in the struct. If composite key case, set comma-separated fields in key sequence.
     * @return Spanner delete Mutation object.
     */
    public static Mutation delete(final Struct struct, final String table, final String keyFields) {
        Key.Builder builder = Key.newBuilder();
        for(final String keyField : keyFields.split(",")) {
            if(struct.isNull(keyField)) {
                throw new IllegalArgumentException(String.format("KeyField: %s must not be null!", keyField));
            }
            switch(struct.getColumnType(keyField).getCode()) {
                case STRING:
                    builder = builder.append(struct.getString(keyField));
                    break;
                case BYTES:
                    builder = builder.append(struct.getBytes(keyField));
                    break;
                case BOOL:
                    builder = builder.append(struct.getBoolean(keyField));
                    break;
                case INT64:
                    builder = builder.append(struct.getLong(keyField));
                    break;
                case FLOAT64:
                    builder = builder.append(struct.getDouble(keyField));
                    break;
                case DATE:
                    builder = builder.append(struct.getDate(keyField));
                    break;
                case TIMESTAMP:
                    builder = builder.append(struct.getTimestamp(keyField));
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "field: %s, fieldType: %s at table %s, is impossible as Key.",
                            keyField, struct.getColumnType(keyField).toString(), table));
            }
        }
        return Mutation.delete(table, builder.build());
    }

    private static Mutation.WriteBuilder createMutationWriteBuilder(final String table, final Mutation.Op mutationOp) {
        switch(mutationOp) {
            case INSERT:
                return Mutation.newInsertBuilder(table);
            case UPDATE:
                return Mutation.newUpdateBuilder(table);
            case INSERT_OR_UPDATE:
                return Mutation.newInsertOrUpdateBuilder(table);
            case REPLACE:
                return Mutation.newReplaceBuilder(table);
            case DELETE:
                throw new IllegalArgumentException("MutationOP(for insert) must not be DELETE!");
            default:
                return Mutation.newInsertOrUpdateBuilder(table);
        }
    }

}