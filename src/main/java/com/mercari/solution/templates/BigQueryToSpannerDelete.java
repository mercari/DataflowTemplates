/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.dofns.StructToMutationDoFn;
import com.mercari.solution.util.converter.RecordToStructConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.ParDo;

/**
 * <p>BigQueryToSpannerDelete template inserts BigQuery query results to specified Cloud Spanner table.</p>
 * <p>
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from BigQuery</td></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner you will recover</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID you will recover.</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID you will recover.</td></tr>
 *     <tr><td>table</td><td>String</td><td>Spanner table name to insert records.</td></tr>
 *     <tr><td>mutationOp</td><td>String</td><td>Spanner insert policy. INSERT or UPDATE or REPLACE or INSERT_OR_UPDATE</td></tr>
 * </table>
 */
public class BigQueryToSpannerDelete {
    private BigQueryToSpannerDelete() {
    }

    public interface BigQueryToSpannerDeletePipelineOption extends PipelineOptions {
        @Description("SQL query to extract records from spanner")
        ValueProvider<String> getQuery();

        void setQuery(ValueProvider<String> query);

        @Description("Project id spanner for store belong to")
        ValueProvider<String> getProjectId();

        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id for store")
        ValueProvider<String> getInstanceId();

        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id for store")
        ValueProvider<String> getDatabaseId();

        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner table name to store query result")
        ValueProvider<String> getTable();

        void setTable(ValueProvider<String> table);

        @Description("Key fields in query results. If composite key case, set comma-separated fields in key sequence.")
        ValueProvider<String> getKeyFields();

        void setKeyFields(ValueProvider<String> keyFields);
    }

    public static void main(final String[] args) {
        final BigQueryToSpannerDeletePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToSpannerDeletePipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);
        // The BigQuery Storage API is distinct from the existing BigQuery API.
        // You must enable the BigQuery Storage API for your Google Cloud Platform project.
        // TODO
        // Dynamic work re-balancing is not currently supported.
        // As a result, reads might be less efficient in the presence of stragglers.
        // https://issues.apache.org/jira/browse/BEAM-7495
        pipeline
                .apply("QueryBigQuery", BigQueryIO.read(RecordToStructConverter::convert)
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withTemplateCompatibility()
                        .withoutValidation()
                        .withCoder(SerializableCoder.of(Struct.class)))
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
