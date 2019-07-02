/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Mutation;
import com.mercari.solution.transforms.dofns.StructToMutationDoFn;
import com.mercari.solution.transforms.SpannerQueryIO;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * <p>SpannerToSpannerDelete template deletes query results from specified Cloud Spanner table.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner you will delete records</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID you will delete records</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID you will delete records</td></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to extract records to delete from Cloud Spanner</td></tr>
 *     <tr><td>table</td><td>String</td><td>Spanner table name to delete records</td></tr>
 *     <tr><td>keyFields</td><td>String</td><td>Key fields in query results. If composite key case, set comma-separated fields in key sequence</td></tr>
 *     <tr><td>timestampBound</td><td>String</td><td>(Optional) Timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.</td></tr>
 * </table>
 */
public class SpannerToSpannerDelete {

    private SpannerToSpannerDelete() {}

    public interface SpannerToSpannerDeletePipelineOption extends PipelineOptions {

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

        @Description("Spanner table name to delete records")
        ValueProvider<String> getTable();
        void setTable(ValueProvider<String> table);

        @Description("Key fields in query results. If composite key case, set comma-separated fields in key sequence.")
        ValueProvider<String> getKeyFields();
        void setKeyFields(ValueProvider<String> keyFields);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(final String[] args) {

        final SpannerToSpannerDeletePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToSpannerDeletePipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()))
                .apply("ConvertToMutation", ParDo.of(new StructToMutationDoFn(
                        options.getTable(),
                        ValueProvider.StaticValueProvider.of(Mutation.Op.DELETE.name()),
                        options.getKeyFields())))
                .apply("DeleteMutation", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }

}