/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.mercari.solution.util.StructUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Converter converts Cloud Spanner Struct to CSV text row
 */
public class StructToCsvConverter {

    private StructToCsvConverter() {}

    /**
     * Convert Cloud Spanner {@link Struct} object to CSV row string.
     *
     * @param struct Cloud Spanner Struct object
     * @return CSV row string.
     */
    public static String convert(final Struct struct) throws IOException {
        List<Object> objs = struct.getType().getStructFields()
                .stream()
                .map((Type.StructField field) -> StructUtil.getFieldValue(field, struct))
                .collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        try(CSVPrinter printer = new CSVPrinter(sb, CSVFormat.DEFAULT)) {
            printer.printRecord(objs);
            printer.flush();
            return sb.toString().trim();
        }
    }

}