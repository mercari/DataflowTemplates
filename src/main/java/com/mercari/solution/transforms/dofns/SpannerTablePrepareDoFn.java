/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.dofns;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.cloud.spanner.*;
import com.google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;


public class SpannerTablePrepareDoFn extends DoFn<Struct, String> {

    private static final Logger LOG = LoggerFactory.getLogger(SpannerTablePrepareDoFn.class);

    private final ValueProvider<String> projectId;
    private final ValueProvider<String> instanceId;
    private final ValueProvider<String> databaseId;
    private final ValueProvider<String> table;
    private final ValueProvider<String> primaryKeyFields;

    public SpannerTablePrepareDoFn(ValueProvider<String> projectId,
                                   ValueProvider<String> instanceId,
                                   ValueProvider<String> databaseId,
                                   ValueProvider<String> table,
                                   ValueProvider<String> primaryKeyFields) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.table = table;
        this.primaryKeyFields = primaryKeyFields;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws Exception {
        final SpannerOptions options = SpannerOptions.newBuilder()
                .setProjectId(this.projectId.get())
                .build();
        final Spanner spanner = options.getService();
        if(existsTable(spanner, DatabaseId.of(this.projectId.get(), this.instanceId.get(), this.databaseId.get()), this.table.get())) {
            c.output("");
            return;
        }

        final Schema schema = AvroSchemaUtil.convertSchema(c.element());//new Schema.Parser().parse(c.element());
        final String createTableSQL = buildCreateTableSQL(schema);
        final OperationFuture<Void, UpdateDatabaseDdlMetadata> meta = spanner.getDatabaseAdminClient()
                .updateDatabaseDdl(this.instanceId.get(), this.databaseId.get(), Arrays.asList(createTableSQL), null);
        meta.get();
        int waitingSeconds = 0;
        while(!meta.isDone()) {
            Thread.sleep(5 * 1000L);
            LOG.info("waiting...");
            waitingSeconds += 5;
            if(waitingSeconds > 3600) {
                throw new IllegalArgumentException("");
            }
        }
        c.output("");
    }

    private String buildCreateTableSQL(final Schema schema) {
        final String keyFields = this.primaryKeyFields.get();
        if(keyFields == null) {
            throw new IllegalArgumentException("Runtime parameter: primaryKeyFields must not be null!");
        }
        final StringBuilder sb = new StringBuilder(String.format("CREATE TABLE %s ( ", table.get()));
        schema.getFields().stream()
                .filter(f -> isValidColumnType(f.schema()))
                .forEach(f -> sb.append(String.format("%s %s,", f.name(), getColumnType(f.schema()))));
        sb.deleteCharAt(sb.length() - 1);
        sb.append(String.format(") PRIMARY KEY ( %s )", keyFields));
        return sb.toString();
    }

    private static boolean isValidColumnType(final Schema schema) {
        switch (schema.getType()) {
            case RECORD:
            case MAP:
            case NULL:
                return false;
            case ARRAY:
                return isValidColumnType(schema.getElementType());
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return isValidColumnType(childSchema);
            default:
                return true;
        }
    }

    private static String getColumnType(final Schema schema) {
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return "STRING(MAX)";
            case FIXED:
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    return "STRING(MAX)";
                }
                return "BYTES(MAX)";
            case BOOLEAN:
                return "BOOL";
            case FLOAT:
            case DOUBLE:
                return "FLOAT64";
            case INT:
                if (LogicalTypes.date().equals(schema.getLogicalType())) {
                    return "DATE";
                } else if (LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return "STRING(MAX)";
                }
                return "INT64";
            case LONG:
                if (LogicalTypes.timestampMillis().equals(schema.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return "TIMESTAMP";
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return "STRING(MAX)";
                }
                return "INT64";
            case ARRAY:
                return "ARRAY<" + getColumnType(schema.getElementType()) + ">";
            case UNION:
                final Schema childSchema = schema.getTypes().stream()
                        .filter(s -> !s.getType().equals(Schema.Type.NULL))
                        .findAny()
                        .orElseThrow(() -> new IllegalArgumentException("UNION does not have another schema."));
                return getColumnType(childSchema);
            default:
                throw new IllegalArgumentException(String.format("DataType: %s is not supported!", schema.getType().name()));

        }
    }

    private static boolean existsTable(Spanner spanner, DatabaseId databaseId, String table) {
        final DatabaseClient client = spanner.getDatabaseClient(databaseId);
        return client.singleUseReadOnlyTransaction()
                .executeQuery(Statement.newBuilder(
                        "SELECT table_name FROM information_schema.tables WHERE table_name=@table")
                        .bind("table")
                        .to(table)
                        .build())
                .next();
    }
}