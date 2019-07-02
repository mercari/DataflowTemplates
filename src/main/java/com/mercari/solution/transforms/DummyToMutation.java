/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.ReadChannel;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.CharStreams;
import com.google.common.hash.Hashing;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.threeten.bp.Duration;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Dummy Spanner Mutation object generator.
 */
public class DummyToMutation extends PTransform<PBegin, PCollection<Mutation>> {

    private static final Logger LOG = LoggerFactory.getLogger(DummyToMutation.class);

    private static final int RANDOM_NULL_RATE = 20; // 0 to 100

    private final ValueProvider<String> projectId;
    private final ValueProvider<String> instanceId;
    private final ValueProvider<String> databaseId;
    private final ValueProvider<String> tables;
    private final ValueProvider<String> config;
    private final ValueProvider<Long> parallelNum;

    /**
     * Read JDBC resultSet records as PCollection of {@link Struct}.
     *
     * @param projectId Project id for Cloud Spanner to store dummy mutations.
     * @param instanceId Spanner Instance id for store dummy mutations.
     * @param databaseId Spanner Database id for store dummy mutations.
     * @param tables Spanner table names to store dummy mutations.
     * @param config GCS path for putting dummy config yaml file.
     * @param parallelNum Parallel execution num.
     */
    public DummyToMutation(ValueProvider<String> projectId,
                           ValueProvider<String> instanceId,
                           ValueProvider<String> databaseId,
                           ValueProvider<String> tables,
                           ValueProvider<String> config,
                           ValueProvider<Long> parallelNum) {
        this.projectId = projectId;
        this.instanceId = instanceId;
        this.databaseId = databaseId;
        this.tables = tables;
        this.config = config;
        this.parallelNum = parallelNum;
    }

    @Override
    public PCollection<Mutation> expand(PBegin begin) {

        /*
        // TODO Enable when SplittableDoFn dynamic rebalancing resolved: https://issues.apache.org/jira/browse/BEAM-4737
        return begin
                .apply("SupplyTables", Create.ofProvider(this.tables, StringUtf8Coder.of()))
                .apply("ReadTableSchema", ParDo.of(new ReadSchemaDoFn(this.projectId, this.instanceId, this.databaseId)))
                .apply("GenerateDummy", ParDo.of(new SplittableDummyGenerateDoFn(this.config)));
        */

        return begin
                .apply("SupplyTables", Create.ofProvider(this.tables, StringUtf8Coder.of()))
                .apply("ReadTableSchema", ParDo.of(new ReadSchemaDoFn(this.projectId, this.instanceId, this.databaseId)))
                .apply("AddSeqNumber", ParDo.of(new AddSeqNumberDoFn(this.parallelNum)))
                .apply("GroupByKey", GroupByKey.create())
                .apply("GenerateDummy", ParDo.of(new GroupByDummyGenerateDoFn(this.config, this.parallelNum)));
    }


    private class ReadSchemaDoFn extends DoFn<String, TableSchemas> {

        private static final String QUERY_COLUMN_SCHEMA =
                "SELECT a.TABLE_NAME, ARRAY_AGG(STRUCT(a.COLUMN_NAME, a.SPANNER_TYPE,COLUMN_DEFAULT,a.IS_NULLABLE,INDEX_NAME)) AS FIELDS \n" +
                        "FROM INFORMATION_SCHEMA.COLUMNS a \n" +
                        "LEFT OUTER JOIN INFORMATION_SCHEMA.INDEX_COLUMNS b \n" +
                        "ON a.TABLE_NAME = b.TABLE_NAME AND a.COLUMN_NAME = b.COLUMN_NAME \n" +
                        "WHERE a.TABLE_NAME in UNNEST(@tables) \n" +
                        "GROUP BY a.TABLE_NAME";

        private final ValueProvider<String> projectId;
        private final ValueProvider<String> instanceId;
        private final ValueProvider<String> databaseId;

        public ReadSchemaDoFn(ValueProvider<String> projectId,
                              ValueProvider<String> instanceId,
                              ValueProvider<String> databaseId) {
            this.projectId = projectId;
            this.instanceId = instanceId;
            this.databaseId = databaseId;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {

            final Map<String, Long> numberMap = Arrays.stream(c.element().split(","))
                    .collect(Collectors.toMap(s -> s.split(":")[0], s -> Long.valueOf(s.split(":")[1])));

            final Spanner spanner = SpannerOptions.newBuilder()
                    .setRetrySettings(RetrySettings.newBuilder().setTotalTimeout(Duration.ofHours(4)).build())
                    .build()
                    .getService();
            final DatabaseId database = DatabaseId.of(this.projectId.get(),this.instanceId.get(),this.databaseId.get());
            final DatabaseClient client = spanner.getDatabaseClient(database);

            final Statement statement = Statement.newBuilder(QUERY_COLUMN_SCHEMA)
                    .bind("tables")
                    .toStringArray(numberMap.keySet())
                    .build();

            try(final ResultSet resultSet = client.singleUseReadOnlyTransaction().executeQuery(statement)) {
                while (resultSet.next()) {
                    final Struct struct = resultSet.getCurrentRowAsStruct();
                    final String tableName = struct.getString("TABLE_NAME");
                    final TableSchemas tableSchema = new TableSchemas(
                            tableName,
                            numberMap.getOrDefault(tableName,null),
                            struct.getStructList("FIELDS").stream()
                                    .map(s -> new TableSchemas.TableField(
                                            s.getString("COLUMN_NAME"),
                                            s.getString("SPANNER_TYPE"),
                                            !s.isNull("INDEX_NAME") && "PRIMARY_KEY".equals(s.getString("INDEX_NAME")),
                                            "YES".equals(s.getString("IS_NULLABLE"))))
                                    .collect(Collectors.toList()));
                    c.output(tableSchema);
                }
            }
        }
    }


    /*
    // TODO Enable when SplittableDoFn dynamic rebalancing resolved: https://issues.apache.org/jira/browse/BEAM-4737
    public class SplittableDummyGenerateDoFn extends DoFn<TableSchemas, Mutation> {

        private final ValueProvider<String> configVP;
        private Config config;


        public SplittableDummyGenerateDoFn(ValueProvider<String> config) {
            this.configVP = config;
        }

        @Setup
        public void setup() throws IOException {
            final String yamlString = readConfig(this.configVP.get());
            this.config = Config.of(yamlString);
        }

        @ProcessElement
        public void processElement(ProcessContext c, OffsetRangeTracker tracker) {
            final TableSchemas ts = c.element();
            final Map<String, List<String>> rangeMap = this.config.getRangeMap(ts.name);
            final List<DummyGenerator> generators = ts.fields.stream()
                    .map(f -> DummyGenerator.of(f.type, f.name, f.isPrimary, f.isNullable,
                            rangeMap.getOrDefault(f.name, null)))
                    .collect(Collectors.toList());
            for(long i = tracker.currentRestriction().getFrom(); tracker.tryClaim(i); ++i) {
                final Mutation mutation = generate(ts.name, generators, i);
                c.output(mutation);
            }
        }

        @GetInitialRestriction
        public OffsetRange getInitialRange(TableSchemas ts) {
            return new OffsetRange(0L, ts.count);
        }

        @SplitRestriction
        public void splitRestriction(final TableSchemas ts, final OffsetRange restriction, final DoFn.OutputReceiver<OffsetRange> receiver) {
            long middle = (restriction.getFrom() + restriction.getTo()) / 2;
            receiver.output(new OffsetRange(restriction.getFrom(), middle));
            receiver.output(new OffsetRange(middle, restriction.getTo()));
        }

        private String readConfig(String source) throws IOException {
            if(!source.startsWith("gs://")) {
                return source;
            }
            String[] paths = source.replaceFirst("gs://", "").split("/", 2);
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobid = BlobId.of(paths[0], paths[1]);
            try (final ReadChannel readChannel = storage.get(blobid).reader(Blob.BlobSourceOption.generationMatch());
                 final InputStream is = Channels.newInputStream(readChannel)) {

                return CharStreams.toString(new InputStreamReader(is));
            }
        }

        private Mutation generate(String tableName, List<DummyGenerator> generators, long i) {
            Mutation.WriteBuilder builder =  Mutation.newInsertOrUpdateBuilder(tableName);
            for(DummyGenerator generator : generators) {
                builder = generator.putDummyValue(builder, i);
            }
            return builder.build();
        }

    }
    */


    private class AddSeqNumberDoFn extends DoFn<TableSchemas, KV<Long, TableSchemas>> {

        private final ValueProvider<Long> parallelNum;

        public AddSeqNumberDoFn(ValueProvider<Long> parallelNum) {
            this.parallelNum = parallelNum;
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            for(long i = 0; i < this.parallelNum.get(); i++) {
                c.output(KV.of(i, c.element()));
            }
        }

    }


    private class GroupByDummyGenerateDoFn extends DoFn<KV<Long, Iterable<TableSchemas>>, Mutation> {

        private final ValueProvider<String> configVP;
        private final ValueProvider<Long> parallelNum;
        private Config config;


        public GroupByDummyGenerateDoFn(ValueProvider<String> config, ValueProvider<Long> parallelNum) {
            this.configVP = config;
            this.parallelNum = parallelNum;
        }

        @Setup
        public void setup() throws IOException {
            final String yamlString = readConfig(this.configVP.get());
            this.config = yamlString == null ? Config.ofDefault() : Config.of(yamlString);
        }

        @ProcessElement
        public void processElement(ProcessContext c) {
            final long parallelNum = this.parallelNum.get();
            final long parallelSeqNumber = c.element().getKey();
            for(final TableSchemas ts : c.element().getValue()) {
                final int randomRate = this.config.getRandomRateMap().getOrDefault(ts.name, RANDOM_NULL_RATE);
                final Map<String, List<String>> rangeMap = this.config.getRangeMap(ts.name);
                final List<DummyGenerator> generators = ts.fields.stream()
                        .map(f -> DummyGenerator.of(f.type, f.name, f.isPrimary, f.isNullable,
                                rangeMap.getOrDefault(f.name, null), randomRate))
                        .collect(Collectors.toList());
                final long diff = (ts.count / parallelNum);
                final long start = diff * parallelSeqNumber;
                final long end = parallelSeqNumber + 1 == parallelNum ? ts.count : diff * (parallelSeqNumber + 1);
                for(long i = start; i < end; ++i) {
                    final Mutation mutation = generate(ts.name, generators, i);
                    c.output(mutation);
                }
            }
        }

        private String readConfig(String source) throws IOException {
            if(source == null) {
                LOG.info("Initialized without config file");
                return null;
            }
            if(!source.startsWith("gs://")) {
                LOG.info("Initialized with config file: %s", source);
                return source;
            }
            String[] paths = source.replaceFirst("gs://", "").split("/", 2);
            Storage storage = StorageOptions.getDefaultInstance().getService();
            BlobId blobid = BlobId.of(paths[0], paths[1]);
            try (final ReadChannel readChannel = storage.get(blobid).reader(Blob.BlobSourceOption.generationMatch());
                 final InputStream is = Channels.newInputStream(readChannel)) {

                return CharStreams.toString(new InputStreamReader(is));
            } catch (NullPointerException e) { // throwsn NullPo when source not found.
                throw new IllegalArgumentException(String.format("Config path [%s] is not found", source));
            }
        }

        private Mutation generate(String tableName, List<DummyGenerator> generators, long i) {
            Mutation.WriteBuilder builder =  Mutation.newInsertOrUpdateBuilder(tableName);
            for(DummyGenerator generator : generators) {
                builder = generator.putDummyValue(builder, i);
            }
            return builder.build();
        }

    }


    private static class Config {

        public List<TableConfig> tables;

        public Config() {}

        public Map<String, List<String>> getRangeMap(String tableName) {
            return tables.stream()
                    .filter(tableConfig -> tableName.equals(tableConfig.name))
                    .flatMap(tableConfig -> tableConfig.fields.stream())
                    .collect(Collectors.toMap(field -> field.name, field -> field.range));
        }

        public Map<String, Integer> getRandomRateMap() {
            return tables.stream()
                    .collect(Collectors.toMap(table -> table.name, table -> table.randomRate));
        }

        public static class TableConfig {

            public String name;
            public Integer randomRate;
            public List<FieldConfig> fields;

            public TableConfig() {}

            public static class FieldConfig {
                public String name;
                public List<String> range;

                public FieldConfig() {}
            }
        }

        public static Config of(String yamlString) {
            final Yaml yaml = new Yaml();
            return yaml.loadAs(yamlString, Config.class);
        }

        public static Config ofDefault() {
            Config config = new Config();
            config.tables = new ArrayList<>();
            return config;
        }

    }

    @DefaultCoder(AvroCoder.class)
    private static class TableSchemas {
        public String name;
        public Long count;
        public List<TableField> fields;

        public TableSchemas(String name, Long count, List<TableField> fields) {
            this.name = name;
            this.count = count;
            this.fields = fields;
        }

        @DefaultCoder(AvroCoder.class)
        private static class TableField {
            public String name;
            public String type;
            public Boolean isPrimary;
            public Boolean isNullable;

            public TableField(String name, String type, Boolean isPrimary, Boolean isNullable) {
                this.name = name;
                this.type = type;
                this.isPrimary = isPrimary;
                this.isNullable = isNullable;
            }
        }

    }

    private interface DummyGenerator {

        Random random = new Random();

        static DummyGenerator of(String fieldType, String fieldName, boolean isPrimary, boolean isNullable, List<String> range, int randomRate) {
            if(fieldType.startsWith("STRING") || fieldType.startsWith("BYTES")) {
                final String sizeString = fieldType.substring(fieldType.indexOf("(") + 1, fieldType.indexOf(")"));
                final Integer lengthLimit = "MAX".equals(sizeString) ? Integer.MAX_VALUE : Integer.valueOf(sizeString);
                if(fieldType.startsWith("BYTES")) {
                    return new BytesDummyGenerator(fieldName, lengthLimit, isPrimary, isNullable, randomRate);
                }
                return new StringDummyGenerator(fieldName, lengthLimit, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("INT64")) {
                return new IntDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("FLOAT64")) {
                return new FloatDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("BOOL")) {
                return new BoolDummyGenerator(fieldName, isNullable, randomRate);
            } else if(fieldType.equals("DATE")) {
                return new DateDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.equals("TIMESTAMP")) {
                return new TimestampDummyGenerator(fieldName, range, isPrimary, isNullable, randomRate);
            } else if(fieldType.startsWith("ARRAY")) {
                final String arrayType = fieldType.substring(fieldType.indexOf("<") + 1, fieldType.indexOf(">"));
                final DummyGenerator elementGenerator = of(arrayType, fieldName, false, isNullable, range, randomRate);
                return new ArrayDummyGenerator(fieldName, elementGenerator, isNullable, randomRate);
            }
            throw new IllegalArgumentException(String.format("Illegal fieldType: %s, %s", fieldType, fieldName));
        }

        static boolean randomNull(boolean isNullable, int randomRate) {
            return isNullable && random.nextInt(100) < randomRate;
        }

        static <T> List<T> generateValues(DummyGenerator generator, long value) {
            final List<T> randomValues = new ArrayList<>();
            for(int i=0; i<10; i++) {
                final T randomValue = (T)generator.generateValue(value);
                randomValues.add(randomValue);
            }
            return randomValues;
        }

        Object generateValue(long value);

        Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value);

        Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value);

    }

    private static class StringDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final List<String> range;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public StringDummyGenerator(String fieldName, Integer maxLength, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.range = range;
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public String generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final String randomString = range != null && range.size() > 0 ?
                    range.get(random.nextInt(range.size())) :
                    UUID.randomUUID().toString() + UUID.randomUUID().toString();
            return maxLength > randomString.length() ? randomString : randomString.substring(0, maxLength);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            final String randomString = generateValue(value);
            return builder.set(this.fieldName).to(randomString);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<String> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toStringArray(randomValues);
        }

    }

    private static class BytesDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final Integer maxLength;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public BytesDummyGenerator(String fieldName, Integer maxLength, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.maxLength = maxLength;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public ByteArray generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final byte[] randomBytes = this.isPrimary ? Long.toString(value).getBytes() : Hashing.sha512().hashLong(value).asBytes();
            return ByteArray.copyFrom(randomBytes.length < maxLength ? randomBytes : Arrays.copyOf(randomBytes, maxLength));
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<ByteArray> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBytesArray(randomValues);
        }

    }

    private static class IntDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long min;
        private final Long max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public IntDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Long.valueOf(range.get(0)) : 0L;
            this.max = range != null && range.size() > 1 ? Long.valueOf(range.get(1)) : Long.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Long generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? value : Math.abs(random.nextLong()) % (this.max - this.min) + this.min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Long> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toInt64Array(randomValues);
        }
    }

    private static class FloatDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Double min;
        private final Double max;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public FloatDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.min = range != null && range.size() > 0 ? Double.valueOf(range.get(0)) : Double.MIN_VALUE;
            this.max = range != null && range.size() > 1 ? Double.valueOf(range.get(1)) : Double.MAX_VALUE;
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Double generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return isPrimary ? (double)value : random.nextDouble() * (max - min) + min;
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Double> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toFloat64Array(randomValues);
        }

    }

    private static class BoolDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Boolean isNullable;
        private final int randomRate;

        public BoolDummyGenerator(String fieldName, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Boolean generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return this.random.nextBoolean();
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(this.fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            List<Boolean> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toBoolArray(randomValues);
        }

    }

    private static class DateDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Integer bound;
        private final LocalDateTime startDate;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public DateDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDate = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime ldt2 = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = (int)(ChronoUnit.DAYS.between(this.startDate.toLocalDate(), ldt2.toLocalDate()));
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Date generateValue(long value) {
            if(!isPrimary && DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            final LocalDateTime nextDate = this.startDate.plusDays(isPrimary ? value : (long)this.random.nextInt(this.bound));
            return Date.fromYearMonthDay(nextDate.getYear(), nextDate.getMonthValue(), nextDate.getDayOfMonth());
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Date> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toDateArray(randomValues);
        }

    }

    private static class TimestampDummyGenerator implements DummyGenerator {

        private final Random random;
        private final String fieldName;
        private final Long bound;
        private final LocalDateTime startDateTime;
        private final Boolean isPrimary;
        private final Boolean isNullable;
        private final int randomRate;

        public TimestampDummyGenerator(String fieldName, List<String> range, Boolean isPrimary, Boolean isNullable, int randomRate) {
            this.random = new Random();
            this.fieldName = fieldName;
            final String startDateStr = range != null && range.size() > 0 ? range.get(0) : "1970-01-01T00:00:00";
            final String endDateStr = range != null && range.size() > 1 ? range.get(1) : "2020-12-31T23:59:59";
            this.startDateTime = LocalDateTime.parse(startDateStr.length() == 10 ? startDateStr + "T00:00:00" : startDateStr);
            final LocalDateTime endDateTime = LocalDateTime.parse(endDateStr.length() == 10 ? endDateStr + "T00:00:00" : endDateStr);
            this.bound = java.time.Duration.between(this.startDateTime, endDateTime).getSeconds();
            this.isPrimary = isPrimary;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public Timestamp generateValue(long value) {
            final LocalDateTime nextTime;
            if(this.isPrimary) {
                nextTime = this.startDateTime.plusSeconds(value);
                return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            } else if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            } else {
                nextTime = this.startDateTime.plusSeconds(Math.abs(this.random.nextLong()) % this.bound);
                return Timestamp.ofTimeSecondsAndNanos(nextTime.toEpochSecond(ZoneOffset.UTC),0);
            }
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return builder.set(fieldName).to(generateValue(value));
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            final List<Timestamp> randomValues = DummyGenerator.generateValues(this, value);
            return builder.set(this.fieldName).toTimestampArray(randomValues);
        }

    }


    private static class ArrayDummyGenerator implements DummyGenerator {

        private final String fieldName;
        private final DummyGenerator elementGenerator;
        private final Boolean isNullable;
        private final int randomRate;

        public ArrayDummyGenerator(String fieldName, DummyGenerator elementGenerator, Boolean isNullable, int randomRate) {
            this.fieldName = fieldName;
            this.elementGenerator = elementGenerator;
            this.isNullable = isNullable;
            this.randomRate = randomRate;
        }

        public List generateValue(long value) {
            if(DummyGenerator.randomNull(this.isNullable, this.randomRate)) {
                return null;
            }
            return DummyGenerator.generateValues(elementGenerator, value);
        }

        public Mutation.WriteBuilder putDummyValue(Mutation.WriteBuilder builder, long value) {
            return elementGenerator.putDummyValues(builder, value);
        }

        public Mutation.WriteBuilder putDummyValues(Mutation.WriteBuilder builder, long value) {
            throw new IllegalArgumentException(String.format("Field: %s, Impossible Nested Array (Array in Array) for Cloud Spanner.", fieldName));
        }

    }

}