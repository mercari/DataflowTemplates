/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.dofns.StructToMutationDoFn;
import com.mercari.solution.util.converter.RecordToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.ParDo;


/**
 * <p>AvroToSpanner template recovers Spanner table from avro files you made using SpannerToAvro template.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>input</td><td>String</td><td>GCS path for input avro files. prefix must start with <strong>gs://</strong></td></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner you will recover</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID you will recover.</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID you will recover.</td></tr>
 *     <tr><td>table</td><td>String</td><td>Spanner table name to insert records.</td></tr>
 *     <tr><td>mutationOp</td><td>String</td><td>Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE</td></tr>
 * </table>
 */
public class AvroToSpanner {

    private AvroToSpanner() {}

    public interface AvroToSpannerPipelineOption extends PipelineOptions {

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> table);

        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> input);

        @Description("Spanner insert policy. INSERT, UPDATE, REPLACE, or INSERT_OR_UPDATE.")
        @Default.String("INSERT_OR_UPDATE")
        ValueProvider<String> getMutationOp();
        void setMutationOp(ValueProvider<String> mutationOp);

    }

    public static void main(final String[] args) {

        final AvroToSpannerPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(AvroToSpannerPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("ReadAvroFile", AvroIO.parseGenericRecords(RecordToStructConverter::convert)
                        .withCoder(AvroCoder.of(Struct.class))
                        .from(options.getInput()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(options.getTable(), options.getMutationOp())))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }
}