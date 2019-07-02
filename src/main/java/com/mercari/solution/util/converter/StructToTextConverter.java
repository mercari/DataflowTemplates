/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import java.io.IOException;

/**
 * Converter converts Cloud Spanner Struct to text row
 */
public class StructToTextConverter {

    private StructToTextConverter() {}

    /**
     * Convert Cloud Spanner {@link Struct} object to text.
     *
     * @param struct Cloud Spanner Struct object
     * @return Text.
     */
    public static String convert(final Struct struct, String type) {
        if("json".equals(type)) {
            return StructToJsonConverter.convert(struct);
        } else {
            try {
                return StructToCsvConverter.convert(struct);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
