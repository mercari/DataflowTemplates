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
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;


/**
 * <p>AvroToDatastore template inserts records from avro files to Datastore's specified kind.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>input</td><td>String</td><td>GCS path for input avro files. prefix must start with <strong>gs://</strong></td></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Datastore to insert</td></tr>
 *     <tr><td>kind</td><td>String</td><td>Datastore kind name to insert.</td></tr>
 *     <tr><td>keyField</td><td>String</td><td>Unique key field name in avro record.</td></tr>
 *     <tr><td>excludeFromIndexFields</td><td>String</td><td>(Optional) Field names to exclude from index.</td></tr>
 * </table>
 */
public class AvroToDatastore {

    private AvroToDatastore(){}

    public interface AvroToDatastorePipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getInput();
        void setInput(ValueProvider<String> input);

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

        final AvroToDatastorePipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(AvroToDatastorePipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> kind = options.getKind();
        final ValueProvider<String> keyField = options.getKeyField();
        final ValueProvider<String> excludeFromIndexFields = options.getExcludeFromIndexFields();

        pipeline.apply("ReadAvro", AvroIO
                        .parseGenericRecords(r -> RecordToEntityConverter.convert(r, kind, keyField, excludeFromIndexFields))
                        .withCoder(SerializableCoder.of(Entity.class))
                        .from(options.getInput()))
                .apply("StoreDatastore", DatastoreIO.v1().write().withProjectId(options.getProjectId()));

        pipeline.run();
    }
}