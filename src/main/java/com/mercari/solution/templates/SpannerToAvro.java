/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.mercari.solution.transforms.SpannerQueryIO;
import com.mercari.solution.transforms.StructToAvroTransform;
import com.mercari.solution.transforms.WriteFilesFinishTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.options.*;

/**
 * <p>SpannerToAvro template converts query results from Cloud Spanner to Avro format and writes specified GCS path.</p>
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
 *     <tr><td>outputNotify</td><td>String</td><td>(Optional) GCS path to notify job completed.</td></tr>
 *     <tr><td>outputEmpty</td><td>Boolean</td><td>(Optional) Output empty file even when no query results.</td></tr>
 * </table>
 */
public class SpannerToAvro {

    private SpannerToAvro() {}

    public interface SpannerToAvroPipelineOption extends PipelineOptions {

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

        @Description("(Optional) Field in query results to separate records.")
        ValueProvider<String> getSplitField();
        void setSplitField(ValueProvider<String> splitField);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);

        @Description("(Optional) GCS path to notify job completed.")
        ValueProvider<String> getOutputNotify();
        void setOutputNotify(ValueProvider<String> outputNotify);

        @Description("(Optional) If set ture, output empty file even no result.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getOutputEmpty();
        void setOutputEmpty(ValueProvider<Boolean> outputEmpty);

    }

    public static void main(final String[] args) {

        final SpannerToAvroPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToAvroPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final WriteFilesResult<String> writeFilesResult = pipeline
                .apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("StoreAvro", StructToAvroTransform.of(
                        options.getOutput(),
                        options.getSplitField()));

        writeFilesResult.getPerDestinationOutputFilenames()
                .apply("WriteNotificationFile", WriteFilesFinishTransform.of(
                        options.getOutput(),
                        options.getOutputNotify(),
                        options.getOutputEmpty()));

        pipeline.run();
    }

}