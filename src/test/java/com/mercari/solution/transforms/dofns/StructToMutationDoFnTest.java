/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.dofns;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.DummyDataSupplier;
import com.mercari.solution.util.converter.StructToMutationConverter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test case for the {@link StructToMutationDoFn} class. */
@RunWith(JUnit4.class)
public class StructToMutationDoFnTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() {

        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        Struct struct2 = DummyDataSupplier.createNestedStruct(false);
        ValueProvider<String> table = ValueProvider.StaticValueProvider.of("table1");
        ValueProvider<String> mutationOp = ValueProvider.StaticValueProvider.of("INSERT");

        PCollection<Mutation> mutations =  pipeline
                .apply("SupplyDummyStruct", Create.of(struct1, struct2))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(table, mutationOp)));

        PAssert.that(mutations).containsInAnyOrder(
                StructToMutationConverter.convert(struct1, "table1", Mutation.Op.INSERT),
                StructToMutationConverter.convert(struct2, "table1", Mutation.Op.INSERT));

        pipeline.run();

    }

}