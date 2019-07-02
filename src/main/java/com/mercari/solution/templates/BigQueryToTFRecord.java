/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.mercari.solution.transforms.WriteFilesFinishTransform;
import com.mercari.solution.util.converter.RecordToTFRecordConverter;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.WriteFilesResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;

/**
 * <p>BigQueryToTFRecord template convert BigQuery query results to TFRecords and store it to specified GCS path.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>query</td><td>String</td><td>SQL query to read records from BigQuery</td></tr>
 *     <tr><td>output</td><td>String</td><td>GCS path to output. prefix must start with gs://</td></tr>
 *     <tr><td>splitField</td><td>String</td><td>Field in query results to separate records</td></tr>
 * </table>
 */
public class BigQueryToTFRecord {

    private BigQueryToTFRecord(){}

    public interface BigQueryToTFRecordPipelineOption extends PipelineOptions {

        @Description("SQL Query text to read records from BigQuery")
        ValueProvider<String> getQuery();
        void setQuery(ValueProvider<String> query);

        @Description("GCS path to store tfrecord")
        ValueProvider<String> getOutput();
        void setOutput(ValueProvider<String> output);

        @Description("Field in query results to separate records.")
        ValueProvider<String> getSplitField();
        void setSplitField(ValueProvider<String> splitField);

        @Description("(Optional) GCS path to notify job completed.")
        ValueProvider<String> getOutputNotify();
        void setOutputNotify(ValueProvider<String> outputNotify);

        @Description("(Optional) If set ture, output empty file even no result.")
        @Default.Boolean(false)
        ValueProvider<Boolean> getOutputEmpty();
        void setOutputEmpty(ValueProvider<Boolean> outputEmpty);
    }

    public static void main(final String[] args) {

        final BigQueryToTFRecordPipelineOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(BigQueryToTFRecordPipelineOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        final ValueProvider<String> keyFieldVP = options.getSplitField();
        final ValueProvider<String> outputVP = options.getOutput();

        // The BigQuery Storage API is distinct from the existing BigQuery API.
        // You must enable the BigQuery Storage API for your Google Cloud Platform project.
        // TODO
        // Dynamic work re-balancing is not currently supported.
        // As a result, reads might be less efficient in the presence of stragglers.
        // https://issues.apache.org/jira/browse/BEAM-7495
        final WriteFilesResult<String> writeFilesResult = pipeline
                .apply("QueryBigQuery", BigQueryIO
                        .read((SchemaAndRecord sr) -> {
                            final String keyField = keyFieldVP.get();
                            if(keyField == null) {
                                return KV.of("", RecordToTFRecordConverter.convert(sr));
                            }
                            final String key = sr.getRecord().get(keyField).toString();
                            return KV.of(key, RecordToTFRecordConverter.convert(sr));
                        })
                        .fromQuery(options.getQuery())
                        .usingStandardSql()
                        .withMethod(BigQueryIO.TypedRead.Method.DIRECT_READ)
                        .withQueryPriority(BigQueryIO.TypedRead.QueryPriority.INTERACTIVE)
                        .withTemplateCompatibility()
                        .withoutValidation()
                        .withCoder(KvCoder.of(StringUtf8Coder.of(), ByteArrayCoder.of())))
                .apply("WriteTFRecord", FileIO
                        .<String, KV<String, byte[]>>writeDynamic()
                        .by(kv -> kv.getKey())
                        .to(outputVP)
                        .withNaming(key -> FileIO.Write.defaultNaming(outputVP.get() + key, ".tfrecord"))
                        .via(Contextful.fn(kv -> kv.getValue()), TFRecordIO.sink())
                        .withCompression(Compression.GZIP)
                        .withDestinationCoder(StringUtf8Coder.of()));

        writeFilesResult.getPerDestinationOutputFilenames()
                .apply("WriteNotificationFile", WriteFilesFinishTransform.of(
                        options.getOutput(),
                        options.getOutputNotify(),
                        options.getOutputEmpty()));

        pipeline.run();
    }
}