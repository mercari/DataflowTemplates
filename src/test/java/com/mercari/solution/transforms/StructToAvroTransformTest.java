/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.joda.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;

public class StructToAvroTransformTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();


    private final TemporaryFolder tmpDir = new TemporaryFolder();

    @Before
    public void setUp() throws IOException {
        System.out.println("setup");
        this.tmpDir.create();
    }

    @After
    public void tearDown() {
        System.out.println("teardown");
        this.tmpDir.delete();
    }

    @Test
    public void test1Type() throws IOException {

        final Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        final Date date2 = Date.fromYearMonthDay(2018, 10, 1);
        final String str1 = "2018-09-01T12:00+09:00";
        final String str2 = "2018-10-01T12:00+09:00";
        final Instant instant1 = Instant.parse(str1);
        final Instant instant2 = Instant.parse(str2);
        final Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);
        final Timestamp timestamp2 = Timestamp.ofTimeMicroseconds(instant2.getMillis() * 1000);

        final Struct struct1 = Struct.newBuilder()
                .set("key").to("struct1")
                .set("bf").to(true)
                .set("if").to(12)
                .set("ff").to(0.005)
                .set("df").to(date1)
                .set("tf").to(timestamp1)
                .set("sf").to("This is a pen")
                .set("nsf").to((String)null)
                .set("asf").toStringArray(Arrays.asList("a", "b", "c"))
                .set("aif").toInt64Array(Arrays.asList(1L, 2L, 3L))
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("key").to("struct2")
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("sf").to("I am a pen")
                .set("nsf").to((String)null)
                .set("asf").toStringArray(Arrays.asList("d", "e", "f"))
                .set("aif").toInt64Array(Arrays.asList(4L, 5L, 6L))
                .build();

        final File outputDir = this.tmpDir.newFolder();

        final ValueProvider<String> output = ValueProvider.StaticValueProvider.of(outputDir.getPath());
        final ValueProvider<String> key = ValueProvider.StaticValueProvider.of(null);

        pipeline.apply("CreateDummy", Create.of(struct1, struct2))
                .apply("TransformAndStore", StructToAvroTransform.of(output, key));

        pipeline.run();

        final FilenameFilter filter1 = (File file, String name) -> name.endsWith("00000-of-00002.avro");
        GenericRecord record1 = getGenericRecord(outputDir, filter1);
        System.out.println(record1);

        final FilenameFilter filter2 = (File file, String name) -> name.endsWith("00001-of-00002.avro");
        GenericRecord record2 = getGenericRecord(outputDir, filter2);
        System.out.println(record2);
    }

    @Test
    public void test2Type() throws IOException {
        final Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        final Date date2 = Date.fromYearMonthDay(2018, 10, 1);
        final String str1 = "2018-09-01T12:00+09:00";
        final String str2 = "2018-10-01T12:00+09:00";
        final Instant instant1 = Instant.parse(str1);
        final Instant instant2 = Instant.parse(str2);
        final Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);
        final Timestamp timestamp2 = Timestamp.ofTimeMicroseconds(instant2.getMillis() * 1000);

        final Struct struct1 = Struct.newBuilder()
                .set("key").to("struct1")
                .set("cbf").to(true)
                .set("cif").to(12)
                .set("cff").to(0.005)
                .set("cdf").to(date1)
                .set("ctf").to(timestamp1)
                .set("csf").to("This is a pen")
                .set("cnsf").to((String)null)
                .build();

        final Type.StructField f0 = Type.StructField.of("key", Type.string());
        final Type.StructField f1 = Type.StructField.of("cbf", Type.bool());
        final Type.StructField f2 = Type.StructField.of("cif", Type.int64());
        final Type.StructField f3 = Type.StructField.of("cff", Type.float64());
        final Type.StructField f4 = Type.StructField.of("cdf", Type.date());
        final Type.StructField f5 = Type.StructField.of("ctf", Type.timestamp());
        final Type.StructField f6 = Type.StructField.of("csf", Type.string());
        final Type.StructField f7 = Type.StructField.of("cnsf", Type.string());

        final Struct struct2 = Struct.newBuilder()
                .set("key").to("struct2")
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("sf").to("I am a pen")
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("rf").to(struct1)
                .set("arf").toStructArray(Type.struct(f0, f1, f2, f3, f4, f5, f6, f7), Arrays.asList(struct1))
                .set("asf").toStringArray(Arrays.asList("a", "b", "c"))
                .set("aif").toInt64Array(Arrays.asList(1L, 2L, 3L))
                .build();

        final File outputDir = this.tmpDir.newFolder();

        final ValueProvider<String> output = ValueProvider.StaticValueProvider.of(outputDir.getPath());
        final ValueProvider<String> key = ValueProvider.StaticValueProvider.of("key");

        pipeline.apply("CreateDummy", Create.of(struct1, struct2))
                .apply("TransformAndStore", StructToAvroTransform.of(output, key));

        pipeline.run();

        final FilenameFilter filter1 = (File file, String name) -> name.endsWith(".avro") && name.startsWith("struct1");
        GenericRecord record1 = getGenericRecord(outputDir, filter1);
        System.out.println(record1);

        final FilenameFilter filter2 = (File file, String name) -> name.endsWith(".avro") && name.startsWith("struct2");
        GenericRecord record2 = getGenericRecord(outputDir, filter2);
        System.out.println(record2);

    }

    private GenericRecord getGenericRecord(final File outputDir, final FilenameFilter filter) throws IOException {
        for(String outputFilePath : outputDir.list(filter)) {
            final String outputFileFullPath = String.format("%s/%s", outputDir.getPath(), outputFilePath);
            final DatumReader<GenericRecord> dataReader = new GenericDatumReader<>();
            final DataFileReader<GenericRecord> recordReader = new DataFileReader<GenericRecord>(new File(outputFileFullPath), dataReader);
            Schema schema = ((GenericDatumReader<GenericRecord>) dataReader).getSchema();
            System.out.println(schema);
            while (recordReader.hasNext()) {
                GenericRecord record = recordReader.next();
                return record;
            }
        }
        return null;
    }
}