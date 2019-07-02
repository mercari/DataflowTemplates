/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.dofns;

import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/** Test case for the {@link StructToTextDoFn} class. */
@RunWith(JUnit4.class)
public class StructToTextDoFnTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testJson() {
        Struct struct1 = Struct.newBuilder()
                .set("bool").to(true)
                .set("int").to(12)
                .set("string").to("string")
                .set("float").to(10.12)
                .set("timestamp").to(Timestamp.parseTimestamp("2018-01-19T03:24:13Z"))
                .build();
        Struct struct2 = Struct.newBuilder()
                .set("bool").to(false)
                .set("int").to(-10)
                .set("string").to("this is a pen!")
                .set("float").to(0.12)
                .set("timestamp").to(Timestamp.parseTimestamp("2018-10-01T12:00:00Z"))
                .build();

        PCollection<String> lines = pipeline
                .apply("CreateDummy", Create.of(struct1, struct2))
                .apply("ConvertToJson", ParDo.of(new StructToTextDoFn(ValueProvider.StaticValueProvider.of("json"))));

        PAssert.that(lines).containsInAnyOrder(
                "{\"bool\":true,\"int\":12,\"string\":\"string\",\"float\":10.12,\"timestamp\":\"2018-01-19T03:24:13Z\"}",
                "{\"bool\":false,\"int\":-10,\"string\":\"this is a pen!\",\"float\":0.12,\"timestamp\":\"2018-10-01T12:00:00Z\"}");

        pipeline.run();
    }

    @Test
    public void testCsv() {
        Struct struct1 = Struct.newBuilder()
                .set("bool").to(true)
                .set("int").to(12)
                .set("string").to("string")
                .set("float").to(10.12)
                .set("date").to(Date.fromYearMonthDay(2018, 9, 1))
                .set("timestamp").to(Timestamp.parseTimestamp("2018-01-19T03:24:13Z"))
                .set("nullstring").to((String)null)
                .build();
        Struct struct2 = Struct.newBuilder()
                .set("bool").to(false)
                .set("int").to(-10)
                .set("string").to("this is a pen!")
                .set("nullstring").to((String)null)
                .set("float").to(0.12)
                .set("timestamp").to(Timestamp.parseTimestamp("2018-10-01T12:00:00Z"))
                .build();

        PCollection<String> lines = pipeline
                .apply("CreateDummy", Create.of(struct1, struct2))
                .apply("ConvertToCsv", ParDo.of(new StructToTextDoFn(ValueProvider.StaticValueProvider.of("csv"))));

        PAssert.that(lines).containsInAnyOrder(
                "true,12,string,10.12,2018-09-01,2018-01-19T03:24:13Z,",
                "false,-10,this is a pen!,,0.12,2018-10-01T12:00:00Z");

        pipeline.run();
    }

    @Test
    public void testNestedJson() {
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
                .set("naif").toInt64Array((long[])null)
                .build();

        final Struct struct2 = Struct.newBuilder()
                .set("key").to("struct2")
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("sf").to("I am a pen")
                .set("of").to(struct1)
                .build();

        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline
                .apply("CreateDummy", Create.of(struct1, struct2))
                .apply("ConvertToJson", ParDo.of(new StructToTextDoFn(ValueProvider.StaticValueProvider.of("json"))));

        PAssert.that(lines).containsInAnyOrder(
                "{\"key\":\"struct1\",\"bf\":true,\"if\":12,\"ff\":0.005,\"df\":\"2018-09-01\",\"tf\":\"2018-09-01T03:00:00Z\",\"sf\":\"This is a pen\",\"nsf\":null,\"asf\":[\"a\",\"b\",\"c\"],\"aif\":[1,2,3],\"naif\":null}",
                "{\"key\":\"struct2\",\"bf\":false,\"if\":-12,\"ff\":110.005,\"df\":\"2018-10-01\",\"tf\":\"2018-10-01T03:00:00Z\",\"sf\":\"I am a pen\",\"of\":{\"key\":\"struct1\",\"bf\":true,\"if\":12,\"ff\":0.005,\"df\":\"2018-09-01\",\"tf\":\"2018-09-01T03:00:00Z\",\"sf\":\"This is a pen\",\"nsf\":null,\"asf\":[\"a\",\"b\",\"c\"],\"aif\":[1,2,3],\"naif\":null}}");

        pipeline.run();
    }

}