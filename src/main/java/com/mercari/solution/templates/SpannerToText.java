/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.SpannerQueryIO;
import com.mercari.solution.transforms.WriteFilesFinishTransform;
import com.mercari.solution.transforms.sinks.TextDynamicSink;
import com.mercari.solution.util.FixedFileNaming;
import com.mercari.solution.util.StorageUtil;
import com.mercari.solution.util.StructUtil;
import com.mercari.solution.util.converter.StructToTextConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.Contextful;

/**
 * <p>SpannerToText template converts query results from Cloud Spanner to text format(csv or newline-delimited json) and writes specified GCS path.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner you will query</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID you will query.</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID you will query.</td></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from Cloud Spanner</td></tr>
 *     <tr><td>output</td><td>String</td><td>GCS path to output. prefix must start with gs://</td></tr>
 *     <tr><td>timestampBound</td><td>String</td><td>(Optional) Timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.</td></tr>
 * </table>
 */
public class SpannerToText {

    private SpannerToText() {}

    public interface SpannerToTextPipelineOption extends PipelineOptions {

        @Description("Format type, choose csv or json. default is json.")
        @Default.String("json")
        ValueProvider<String> getType();
        void setType(ValueProvider<String> type);

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to output. prefix must start with gs://")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);

        @Description("(Optional) Field in query results to separate records.")
        ValueProvider<String> getSplitField();
        void setSplitField(ValueProvider<String> splitField);

        @Description("(Optional) GCS path to notify job completed.")
        ValueProvider<String> getOutputNotify();
        void setOutputNotify(ValueProvider<String> outputNotify);

        @Description("(Optional) If set ture, output empty file even no result.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getOutputEmpty();
        void setOutputEmpty(ValueProvider<Boolean> outputEmpty);

        @Description("(Optional) Turn off sharding to fix filename as you specified at output.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getWithoutSharding();
        void setWithoutSharding(ValueProvider<Boolean> withoutSharding);

        @Description("(Optional) The number of output shards produced overall.")
        ValueProvider<String> getHeader();
        void setHeader(ValueProvider<String> header);

    }

    public static void main(final String[] args) {

        final SpannerToTextPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToTextPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> type = options.getType();
        final ValueProvider<String> output = options.getOutput();
        final ValueProvider<String> header = options.getHeader();
        final ValueProvider<String> splitField = options.getSplitField();
        final ValueProvider<Boolean> withoutSharding = options.getWithoutSharding();

        final WriteFilesResult<String> writeFilesResult = pipeline
                .apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("WriteText", FileIO.<String, Struct>writeDynamic()
                        .by(Contextful.fn(struct -> {
                            final Object value = StructUtil.getFieldValue(splitField.get(), struct);
                            return value == null ? "" : value.toString();
                        }))
                        .via(Contextful.fn(key -> TextDynamicSink.of(type.get(), header.get(),
                                (Struct struct, String t) -> StructToTextConverter.convert(struct, t))))
                        .to(ValueProvider.NestedValueProvider.of(options.getOutput(), s -> StorageUtil.removeDirSuffix(s)))
                        .withNumShards(ValueProvider.NestedValueProvider.of(withoutSharding, ws -> ws ? 1 : 0))
                        .withNaming(key -> withoutSharding.get() ?
                                FixedFileNaming.of(StorageUtil.addFilePrefix(output.get(), key), type.get()) :
                                FileIO.Write.defaultNaming(StorageUtil.addFilePrefix(output.get(), key), "." + type.get()))
                        .withDestinationCoder(StringUtf8Coder.of()));

        writeFilesResult.getPerDestinationOutputFilenames()
                .apply("WriteNotificationFile", WriteFilesFinishTransform.of(
                        options.getOutput(),
                        options.getOutputNotify(),
                        options.getOutputEmpty(),
                        options.getHeader(),
                        options.getType()));

        pipeline.run();
    }
}