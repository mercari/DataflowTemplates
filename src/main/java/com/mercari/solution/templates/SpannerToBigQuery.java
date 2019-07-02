/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.transforms.SpannerQueryIO;
import com.mercari.solution.util.converter.StructToTableRowConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.Map;


/**
 * <p>SpannerToBigQuery template inserts query results from Cloud Spanner to specified BigQuery table.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner you will query</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID you will query.</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID you will query.</td></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from Cloud Spanner</td></tr>
 *     <tr><td>output</td><td>String</td><td>Destination BigQuery table. format {dataset}.{table}</td></tr>
 *     <tr><td>timestampBound</td><td>String</td><td>(Optional) Timestamp bound (format: yyyy-MM-ddTHH:mm:SSZ). default is strong.</td></tr>
 * </table>
 */
public class SpannerToBigQuery {

    private SpannerToBigQuery() {}

    public interface SpannerToBigQueryPipelineOption extends PipelineOptions {

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

        @Description("Destination BigQuery table. format {dataset}.{table}")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("(Optional) Input timestamp bound as format 'yyyy-MM-ddTHH:mm:SSZ'")
        ValueProvider<String> getTimestampBound();
        void setTimestampBound(ValueProvider<String> timestampBound);
    }

    public static void main(final String[] args) {

        final SpannerToBigQueryPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(SpannerToBigQueryPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> output = options.getOutput();

        final PCollection<Struct> structs = pipeline
                .apply("QuerySpanner", SpannerQueryIO.read(
                        options.getProjectId(),
                        options.getInstanceId(),
                        options.getDatabaseId(),
                        options.getQuery(),
                        options.getTimestampBound()));

        final PCollectionView<Map<String,String>> schemaView = structs
                .apply("SampleStruct", Sample.any(1))
                .apply("AsMap", MapElements
                        .into(TypeDescriptors.maps(TypeDescriptors.strings(),TypeDescriptors.strings()))
                        .via(struct -> StructToTableRowConverter.convertSchema(output.get(), struct)))
                .apply("AsView", View.asSingleton());

        structs.apply("WriteBigQuery", BigQueryIO.<Struct>write()
                        .to(options.getOutput())
                        .withFormatFunction(StructToTableRowConverter::convert)
                        .withSchemaFromView(schemaView)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        pipeline.run();
    }

}