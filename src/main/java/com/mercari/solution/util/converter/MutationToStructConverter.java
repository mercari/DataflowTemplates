/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.ValueBinder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converter converts Spanner Mutation to Spanner Struct
 */
public class MutationToStructConverter {

    private MutationToStructConverter() {}

    /**
     * Convert Spanner {@link MutationGroup} to List of Spanner {@link Struct}.
     *
     * @param mutationGroup Spanner MutationGroup to be converted to List of Struct object.
     * @return List of Struct converted from MutationGroup.
     */
    public static List<Struct> convert(final MutationGroup mutationGroup) {
        final List<Struct> structs = mutationGroup.attached().stream()
                .map(MutationToStructConverter::convert)
                .collect(Collectors.toList());
        structs.add(convert(mutationGroup.primary()));
        return structs;
    }

    /**
     * Convert Spanner {@link Mutation} to List of Spanner {@link Struct}.
     *
     * @param mutation Spanner Mutation to be converted to Spanner Struct object.
     * @return Struct converted from Mutation.
     */
    public static Struct convert(final Mutation mutation) {
        Struct.Builder builder = Struct.newBuilder();
        for(final Map.Entry<String, Value> column : mutation.asMap().entrySet()) {
            final Value value = column.getValue();
            final ValueBinder<Struct.Builder> binder = builder.set(column.getKey());
            switch(value.getType().getCode()) {
                case DATE:
                    builder = binder.to(value.isNull() ? null : value.getDate());
                    break;
                case INT64:
                    builder = binder.to(value.isNull() ? null : value.getInt64());
                    break;
                case STRING:
                    builder = binder.to(value.isNull() ? null : value.getString());
                    break;
                case TIMESTAMP:
                    builder = binder.to(value.isNull() ? null : value.getTimestamp());
                    break;
                case BOOL:
                    builder = binder.to(value.isNull() ? null : value.getBool());
                    break;
                case BYTES:
                    builder = binder.to(value.isNull() ? null : value.getBytes());
                    break;
                case FLOAT64:
                    builder = binder.to(value.isNull() ? null : value.getFloat64());
                    break;
                case STRUCT:
                    builder = binder.to(value.isNull() ? null : value.getStruct());
                    break;
                case ARRAY:
                    switch (value.getType().getArrayElementType().getCode()) {
                        case DATE:
                            builder = binder.toDateArray(value.isNull() ? null : value.getDateArray());
                            break;
                        case INT64:
                            builder = binder.toInt64Array(value.isNull() ? null : value.getInt64Array());
                            break;
                        case STRING:
                            builder = binder.toStringArray(value.isNull() ? null : value.getStringArray());
                            break;
                        case TIMESTAMP:
                            builder = binder.toTimestampArray(value.isNull() ? null : value.getTimestampArray());
                            break;
                        case BOOL:
                            builder = binder.toBoolArray(value.isNull() ? null : value.getBoolArray());
                            break;
                        case BYTES:
                            builder = binder.toBytesArray(value.isNull() ? null : value.getBytesArray());
                            break;
                        case FLOAT64:
                            builder = binder.toFloat64Array(value.isNull() ? null : value.getFloat64Array());
                            break;
                    }
                    break;
            }
        }
        return builder.build();
    }

}