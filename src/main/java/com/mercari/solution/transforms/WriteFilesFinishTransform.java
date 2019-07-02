/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms;

import com.mercari.solution.util.StorageUtil;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WriteFilesFinishTransform extends PTransform<PCollection<KV<String, String>>, PDone> {

    private final ValueProvider<String> output;
    private final ValueProvider<String> outputNotify;
    private final ValueProvider<Boolean> outputEmpty;
    private final ValueProvider<String> emptyText;
    private final ValueProvider<String> suffix;

    private WriteFilesFinishTransform(ValueProvider<String> output,
                                      ValueProvider<String> outputNotify,
                                      ValueProvider<Boolean> outputEmpty,
                                      ValueProvider<String> emptyText,
                                      ValueProvider<String> suffix) {
        this.output = output;
        this.outputNotify = outputNotify;
        this.outputEmpty = outputEmpty;
        this.emptyText = emptyText;
        this.suffix = suffix;
    }

    @Override
    public PDone expand(PCollection<KV<String, String>> outfiles) {
        outfiles.apply("ExtractOutputPath", MapElements.into(TypeDescriptors.strings()).via(s -> s.getValue()))
                        .setCoder(NullableCoder.of(StringUtf8Coder.of()))
                .apply("GloballyAggregate", Combine.globally((a, b) -> a + "\n" + b))
                .apply("WriteOutputPaths", ParDo.of(new DoFn<String, Void>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        final String text = c.element();
                        if(outputEmpty.get() && text == null) {
                            final String etext = emptyText == null || emptyText.get() == null ? "" : emptyText.get();
                            final String path = output.get() + "." + (suffix == null || suffix.get() == null ? "txt" : suffix.get());
                            StorageUtil.createText(path, etext);
                        }
                        final String gcsPath = outputNotify.get();
                        if(gcsPath != null && gcsPath.startsWith("gs://")) {
                            StorageUtil.createText(gcsPath, text == null ? "" : text);
                        }
                    }
                }));

        return PDone.in(outfiles.getPipeline());
    }

    public static WriteFilesFinishTransform of(ValueProvider<String> output,
                                               ValueProvider<String> outputNotify,
                                               ValueProvider<Boolean> outputEmpty) {

        return new WriteFilesFinishTransform(output, outputNotify, outputEmpty, null, null);
    }

    public static WriteFilesFinishTransform of(ValueProvider<String> output,
                                               ValueProvider<String> outputNotify,
                                               ValueProvider<Boolean> outputEmpty,
                                               ValueProvider<String> emptyText) {

        return new WriteFilesFinishTransform(output, outputNotify, outputEmpty, emptyText, null);
    }

    public static WriteFilesFinishTransform of(ValueProvider<String> output,
                                               ValueProvider<String> outputNotify,
                                               ValueProvider<Boolean> outputEmpty,
                                               ValueProvider<String> emptyText,
                                               ValueProvider<String> suffix) {

        return new WriteFilesFinishTransform(output, outputNotify, outputEmpty, emptyText, suffix);
    }

}
