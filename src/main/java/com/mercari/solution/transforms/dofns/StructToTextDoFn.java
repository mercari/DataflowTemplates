/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.dofns;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.converter.StructToCsvConverter;
import com.mercari.solution.util.converter.StructToJsonConverter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;

import java.io.IOException;


public class StructToTextDoFn extends DoFn<Struct, String> {

    private final ValueProvider<String> type;
    private boolean handleJsonType;

    public StructToTextDoFn(ValueProvider<String> type) {
        this.type = type;
    }

    @Setup
    public void setup() {
        this.handleJsonType = !"csv".equals(this.type.get().toLowerCase());
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws IOException {
        final Struct struct = c.element();
        final String out;
        if(this.handleJsonType) {
            out = StructToJsonConverter.convert(struct);
        } else {
            out = StructToCsvConverter.convert(struct);
        }
        c.output(out);
    }

}