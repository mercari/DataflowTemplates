/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.google.datastore.v1.Entity;
import com.mercari.solution.util.converter.RecordToEntityConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.*;

/**
 * <p>BigQueryToDatastore template inserts BigQuery query results to Datastore's specified kind.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from BigQuery</td></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Datastore to insert</td></tr>
 *     <tr><td>kind</td><td>String</td><td>Datastore kind name to insert.</td></tr>
 *     <tr><td>keyField</td><td>String</td><td>Unique key field name in avro record.</td></tr>
 *     <tr><td>excludeFromIndexFields</td><td>String</td><td>(Optional) Field names to exclude from index.</td></tr>
 * </table>
 */
public class BigQueryToDatastore {

    private BigQueryToDatastore() {}

    public interface BigQueryToDatastorePipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("Project ID that datastore is belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> output);

        @Description("Cloud Datastore target kind name to store.")
        ValueProvider<String> getKind();
        void setKind(ValueProvider<String> databaseId);

        @Description("Unique field name in query results from BigQuery.")
        ValueProvider<String> getKeyField();
        void setKeyField(ValueProvider<String> fieldKey);

        @Description("Field names to exclude from index.")
        ValueProvider<String> getExcludeFromIndexFields();
        void setExcludeFromIndexFields(ValueProvider<String> excludeFromIndexFields);

    }

    public static void main(final String[] args) {

        final BigQueryToDatastorePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToDatastorePipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> kind = options.getKind();
        final ValueProvider<String> keyField = options.getKeyField();
        final ValueProvider<String> excludeFromIndexFields = options.getExcludeFromIndexFields();

        // The BigQuery Storage API is distinct from the existing BigQuery API.
        // You must enable the BigQuery Storage API for your Google Cloud Platform project.
        // TODO
        // Dynamic work re-balancing is not currently supported.
        // As a result, reads might be less efficient in the presence of stragglers.
        // https://issues.apache.org/jira/browse/BEAM-7495
        pipeline.apply("QueryBigQuery", BigQueryIO
                        .read(r -> RecordToEntityConverter.convert(r, kind, keyField, excludeFromIndexFields))
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withTemplateCompatibility()
                        .withCoder(SerializableCoder.of(Entity.class))
                        .withoutValidation())
                .apply("StoreDatastore", DatastoreIO.v1().write().withProjectId(options.getProjectId()));
        
        pipeline.run();
    }

}