/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.templates;

import com.mercari.solution.transforms.DummyToMutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.*;

/**
 * <p>DummyToSpanner template generates dummy records according to specified Spanner's table schema and inserts the table.</p>
 *
 * Template parameters are as follows.
 *
 * <table summary="summary" border="1" cellpadding="3" cellspacing="0">
 *     <tr><th>Parameter</th><th>Type</th><th>Description</th></tr>
 *     <tr><td>projectId</td><td>String</td><td>Project ID for Spanner to insert dummy records.</td></tr>
 *     <tr><td>instanceId</td><td>String</td><td>Spanner instanceID to insert dummy records.</td></tr>
 *     <tr><td>databaseId</td><td>String</td><td>Spanner databaseID to insert dummy records.</td></tr>
 *     <tr><td>table</td><td>String</td><td>Spanner table name to insert dummy records.</td></tr>
 *     <tr><td>config</td><td>String</td><td>(Optional) GCS path for dummy config file.</td></tr>
 * </table>
 */
public class DummyToSpanner {

    private DummyToSpanner() {}

    public interface DummyToSpannerOption extends PipelineOptions {

        @Description("Project id spanner instance belong to")
        ValueProvider<String> getProjectId();
        void setProjectId(ValueProvider<String> projectId);

        @Description("Spanner instance id you want to access")
        ValueProvider<String> getInstanceId();
        void setInstanceId(ValueProvider<String> instanceId);

        @Description("Spanner Database id you want to access")
        ValueProvider<String> getDatabaseId();
        void setDatabaseId(ValueProvider<String> databaseId);

        @Description("Spanner Table Names and num of dummy record.")
        ValueProvider<String> getTables();
        void setTables(ValueProvider<String> table);

        @Description("Config yaml file. GCS path or yaml text itself.")
        ValueProvider<String> getConfig();
        void setConfig(ValueProvider<String> config);

        @Description("Worker parallel num to generate dummy records.")
        @Default.Long(1)
        ValueProvider<Long> getParallelNum();
        void setParallelNum(ValueProvider<Long> parallelNum);

    }

    public static void main(final String[] args) {

        final DummyToSpannerOption options = PipelineOptionsFactory
                .fromArgs(args)
                .as(DummyToSpannerOption.class);
        final Pipeline pipeline = Pipeline.create(options);

        pipeline.apply("GenerateDummy", new DummyToMutation(
                options.getProjectId(),
                options.getInstanceId(),
                options.getDatabaseId(),
                options.getTables(),
                options.getConfig(),
                options.getParallelNum()))
                .apply("StoreSpanner", SpannerIO.write()
                        .withProjectId(options.getProjectId())
                        .withInstanceId(options.getInstanceId())
                        .withDatabaseId(options.getDatabaseId()));

        pipeline.run();
    }


}