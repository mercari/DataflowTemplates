/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.sinks.AvroDynamicSink;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.StorageUtil;
import com.mercari.solution.util.StructUtil;
import com.mercari.solution.util.converter.StructToRecordConverter;
import org.apache.avro.Schema;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Avro File Sink processing Spanner Struct as input.
 */
public class StructToAvroTransform extends PTransform<PCollection<Struct>, WriteFilesResult<String>> {

    private static final TupleTag<KV<String,Struct>> tagMain = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };
    private static final TupleTag<KV<String,Struct>> tagView = new TupleTag<KV<String,Struct>>(){ private static final long serialVersionUID = 1L; };

    private final ValueProvider<String> output;
    private final ValueProvider<String> splitField;
    private final ValueProvider<Boolean> useSnappy;

    /**
     * Read spanner records as PCollection of {@link Struct}.
     *
     * @param output GCS prefix path to write avro files.
     * @param splitField (Optional) Set field name if you want to group input struct by the field.
     * @param useSnappy Use snappy coder or not.
     */
    private StructToAvroTransform(ValueProvider<String> output, ValueProvider<String> splitField, ValueProvider<Boolean> useSnappy) {
        this.output = output;
        this.splitField = splitField;
        this.useSnappy = useSnappy;
    }

    public static StructToAvroTransform of(ValueProvider<String> output) {
        return new StructToAvroTransform(output, null, null);
    }

    public static StructToAvroTransform of(ValueProvider<String> output,
                                           ValueProvider<String> splitField) {
        return new StructToAvroTransform(output, splitField, null);
    }

    public static StructToAvroTransform of(ValueProvider<String> output,
                                           ValueProvider<String> splitField,
                                           ValueProvider<Boolean> useSnappy) {
        return new StructToAvroTransform(output, splitField, useSnappy);
    }

    public final WriteFilesResult<String> expand(PCollection<Struct> input) {

        final PCollectionTuple records = input.apply("AddGroupingKey", ParDo.of(new DoFn<Struct, KV<String, Struct>>() {

            private String keyFieldString;
            private Set<String> check;

            @Setup
            public void setup() {
                this.keyFieldString = splitField.get();
                this.check = new HashSet<>();
            }

            @ProcessElement
            public void processElement(ProcessContext c) {
                final Struct struct = c.element();
                final Object value = this.keyFieldString == null ?
                        null : StructUtil.getFieldValue(this.keyFieldString, struct);
                final String key = value == null ? "" : value.toString();
                final KV<String, Struct> kv = KV.of(key, struct);
                c.output(kv);
                if(!this.check.contains(key)) {
                    c.output(tagView, kv);
                    this.check.add(key);
                }
            }

        }).withOutputTags(tagMain, TupleTagList.of(tagView)));

        final PCollectionView<Map<String, Iterable<Struct>>> schemaView = records.get(tagView)
                .apply("SampleStructPerKey", Sample.fixedSizePerKey(1))
                .apply("ViewAsMap", View.asMap());

        return records.get(tagMain)
                .apply("WriteStructDynamically", FileIO.<String, KV<String, Struct>>writeDynamic()
                        .by(Contextful.fn(element -> element.getKey()))
                        .via(Contextful.fn((key, c) -> {
                            final Map<String, Iterable<Struct>> sampleStruct = c.sideInput(schemaView);
                            if(!sampleStruct.containsKey(key) || !sampleStruct.get(key).iterator().hasNext()) {
                                throw new IllegalArgumentException(String.format("No matched struct to key %s !", key));
                            }
                            final Struct struct = sampleStruct.get(key).iterator().next();
                            final Schema schema = AvroSchemaUtil.convertSchema(struct);
                            return AvroDynamicSink.of(schema,
                                    (KV<String, Struct> kv, Schema s)
                                            -> StructToRecordConverter.convert(kv.getValue(), s));
                        }, Requirements.requiresSideInputs(schemaView)))
                        .to(ValueProvider.NestedValueProvider.of(this.output, s -> StorageUtil.removeDirSuffix(s)))
                        .withNaming(key -> FileIO.Write.defaultNaming(
                                StorageUtil.addFilePrefix(this.output.get(), key), ".avro"))
                        .withDestinationCoder(StringUtf8Coder.of()));
    }

}