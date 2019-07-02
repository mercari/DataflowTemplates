/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.dofns;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.converter.StructToMutationConverter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StructToMutationDoFn extends DoFn<Struct, Mutation> {

    private static final Logger LOG = LoggerFactory.getLogger(StructToMutationDoFn.class);

    private final ValueProvider<String> tableVP;
    private final ValueProvider<String> mutationOpVP;
    private final ValueProvider<String> keyFieldsVP;
    private String table;
    private String keyFields = null;
    private Mutation.Op mutationOp;

    public StructToMutationDoFn(ValueProvider<String> tableVP, ValueProvider<String> mutationOpVP) {
        this.tableVP = tableVP;
        this.mutationOpVP = mutationOpVP;
        this.keyFieldsVP = null;
    }

    public StructToMutationDoFn(ValueProvider<String> tableVP, ValueProvider<String> mutationOpVP, ValueProvider<String> keyFieldsVP) {
        this.tableVP = tableVP;
        this.mutationOpVP = mutationOpVP;
        this.keyFieldsVP = keyFieldsVP;
    }

    @Setup
    public void setup() {
        this.table = this.tableVP.get();
        this.mutationOp = Mutation.Op.valueOf(this.mutationOpVP.get());
        if(this.keyFieldsVP != null) {
            this.keyFields = this.keyFieldsVP.get();
        }
        LOG.info(String.format("StructToMutationDoFn setup finished. table:[%s], op:[%s]", this.table, this.mutationOp));
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
        Struct struct = c.element();
        if(Mutation.Op.DELETE.equals(this.mutationOp)) {
            c.output(StructToMutationConverter.delete(struct, this.table, this.keyFields));
        } else {
            c.output(StructToMutationConverter.convert(struct, this.table, this.mutationOp));
        }
    }

}