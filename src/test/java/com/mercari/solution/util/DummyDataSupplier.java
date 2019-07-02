/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import org.joda.time.Instant;

import java.util.Arrays;
import java.util.List;

public class DummyDataSupplier {

    private DummyDataSupplier() {

    }

    public static Struct createSimpleStruct() {
        Date date1 = Date.fromYearMonthDay(2018, 9, 1);
        String str1 = "2018-09-01T12:00+09:00";
        Instant instant1 = Instant.parse(str1);
        Timestamp timestamp1 = Timestamp.ofTimeMicroseconds(instant1.getMillis() * 1000);

        Struct struct1 = Struct.newBuilder()
                .set("cbf").to(true)
                .set("cif").to(12)
                .set("cff").to(0.005)
                .set("cdf").to(date1)
                .set("ctf").to(timestamp1)
                .set("csf").to("This is a pen")
                .set("cnf").to(ByteArray.copyFrom(new byte[]{0,0,0,0,0,0,0,0,0,0,0,0,5,-11,-31,0})) // 0.1
                .build();

        return struct1;
    }

    public static Struct createNestedStruct(boolean containsNullArray) {
        Date date2 = Date.fromYearMonthDay(2018, 10, 1);
        String str2 = "2018-10-01T12:00+09:00";
        Instant instant2 = Instant.parse(str2);
        Timestamp timestamp2 = Timestamp.ofTimeMicroseconds(instant2.getMillis() * 1000);

        Type.StructField f1 = Type.StructField.of("cbf", Type.bool());
        Type.StructField f2 = Type.StructField.of("cif", Type.int64());
        Type.StructField f3 = Type.StructField.of("cff", Type.float64());
        Type.StructField f4 = Type.StructField.of("cdf", Type.date());
        Type.StructField f5 = Type.StructField.of("ctf", Type.timestamp());
        Type.StructField f6 = Type.StructField.of("csf", Type.string());
        Type.StructField f7 = Type.StructField.of("cnf", Type.bytes());

        Struct struct1 = createSimpleStruct();

        Struct struct2 = Struct.newBuilder()
                .set("bf").to(false)
                .set("if").to(-12)
                .set("ff").to(110.005)
                .set("sf").to("I am a pen")
                .set("df").to(date2)
                .set("tf").to(timestamp2)
                .set("nf").to((String)null)
                .set("lnf").to((Long)null)
                .set("dnf").to((Date)null)
                .set("tnf").to((Timestamp)null)
                .set("rf").to(struct1)
                .set("arf").toStructArray(Type.struct(f1,f2, f3, f4, f5, f6, f7), Arrays.asList(struct1))
                .set("asf").toStringArray(Arrays.asList("a", "b", "c"))
                .set("aif").toInt64Array(Arrays.asList(1L, 2L, 3L))
                .set("adf").toDateArray(Arrays.asList(struct1.getDate("cdf"), date2))
                .set("anf").toInt64Array((List<Long>)null)
                .set("amf").toInt64Array(Arrays.asList(containsNullArray ? null : 1L, 2L, 3L))
                .set("atf").toTimestampArray(Arrays.asList(struct1.getTimestamp("ctf"), timestamp2))
                .build();

        return struct2;
    }
}