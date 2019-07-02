/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.dofns.StructToMutationDoFn;
import com.mercari.solution.transforms.SpannerQueryIO;
import com.mercari.solution.transforms.StructToAvroTransform;
import com.mercari.solution.util.converter.MutationToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * <p>SpannerToSpanner template inserts query results from Cloud Spanner to specified BigQuery table.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>inputProjectId</td><td>String</td><td>Project ID for Cloud Spanner you will query</td></tr>
 *     <tr><td>inputInstanceId</td><td>String</td><td>Spanner instanceID you will query.</td></tr>
 *     <tr><td>inputDatabaseId</td><td>String</td><td>Spanner databaseID you will query.</td></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from Cloud Spanner</td></tr>
 *     <tr><td>outputProjectId</td><td>String</td><td>Project id for Cloud Spanner to store query results</td></tr>
 *     <tr><td>outputInstanceId</td><td>String</td><td>Spanner Instance id for store query results</td></tr>
 *     <tr><td>outputDatabaseId</td><td>String</td><td>Spanner Database id for store query results</td></tr>
 *     <tr><td>outputError</td><td>String</td><td>GCS output path to store records where an error occurred during processing</td></tr>
 *     <tr><td>table</td><td>String</td><td>Spanner table name to store query result.</td></tr>
 *     <tr><td>mutationOp</td><td>String</td><td>(Optional) Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE. default is INSERT_OR_UPDATE</td></tr>
 *     <tr><td>timestampBound</td><td>String</td><td>(Optional) Timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.</td></tr>
 * </table>
 */
public class SpannerToSpanner {

    private SpannerToSpanner() {}

    public interface SpannerToSpannerPipelineOption extends PipelineOptions {

        @Description("Project id spanner for query belong to")
        ValueProvider<String> getInputProjectId();
        void setInputProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id for query")
        ValueProvider<String> getInputInstanceId();
        void setInputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for query")
        ValueProvider<String> getInputDatabaseId();
        void setInputDatabaseId(ValueProvider<String> databaseId);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getOutputProjectId();
        void setOutputProjectId(ValueProvider<String> output);

        @Description("Spanner instance id for store")
        ValueProvider<String> getOutputInstanceId();
        void setOutputInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getOutputDatabaseId();
        void setOutputDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> databaseId);

        @Description("GCS output path to store records where an error occurred during processing")
        ValueProvider<String> getOutputError();
        void setOutputError(ValueProvider<String> error);

        @Description("Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);

    }

    public static void main(final String[] args) {

        final SpannerToSpannerPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToSpannerPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final SpannerWriteResult result = pipeline
                .apply("QuerySpanner", SpannerQueryIO.read(
                        options.getInputProjectId(),
                        options.getInputInstanceId(),
                        options.getInputDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withFailureMode(SpannerIO.FailureMode.REPORT_FAILURES)
                        .withProjectId(options.getOutputProjectId())
                        .withInstanceId(options.getOutputInstanceId())
                        .withDatabaseId(options.getOutputDatabaseId()));

        result.getFailedMutations()
                .apply("ErrorMutationToStruct", FlatMapElements
                        .into(TypeDescriptor.of(Struct.class))
                        .via(MutationToStructConverter::convert))
                .apply("StoreErrorStorage", StructToAvroTransform.of(
                        options.getOutputError()));

        pipeline.run();
    }

}