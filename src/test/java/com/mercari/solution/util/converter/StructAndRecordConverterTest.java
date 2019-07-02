/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.Date;
import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.DummyDataSupplier;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.joda.time.*;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.stream.Collectors;

/** Test cases for the {@link StructToRecordConverter} and {@link RecordToStructConverter} class. */
@RunWith(JUnit4.class)
public class StructAndRecordConverterTest {

    private static final MutableDateTime EPOCHDATETIME = new MutableDateTime(0, DateTimeZone.UTC);

    @Test
    public void testSchema() {
        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        //Schema schema1 = StructToRecordConverter.convertSchema(struct1);
        Schema schema2 = AvroSchemaUtil.convertSchema(struct1);
        //System.out.println(schema1);
        System.out.println(schema2);
    }

    @Test
    public void testConvert() {
        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        Struct struct2 = DummyDataSupplier.createNestedStruct(true);
        //Schema schema = StructToRecordConverter.convertSchema(struct2);
        Schema schema = AvroSchemaUtil.convertSchema(struct2);

        // Test struct to record.
        GenericRecord r = StructToRecordConverter.convert(struct2, schema);
        Assert.assertFalse((Boolean)r.get("bf"));
        Assert.assertEquals(struct2.getLong("if"), (long)r.get("if"));
        Assert.assertEquals(struct2.getDouble("ff"), (double)r.get("ff"), 0);
        Assert.assertEquals(struct2.getString("sf"), r.get("sf"));
        Assert.assertEquals(getEpochDays(struct2.getDate("df")), r.get("df"));
        Assert.assertEquals(struct2.getTimestamp("tf").getSeconds()*1000, r.get("tf"));
        Assert.assertArrayEquals(struct2.getLongList("aif").toArray(), ((List<Long>)(r.get("aif"))).toArray());
        Assert.assertNull(r.get("nf"));
        Assert.assertNull(r.get("lnf"));
        Assert.assertNull(r.get("dnf"));
        Assert.assertNull(r.get("tnf"));
        Assert.assertNull(r.get("anf"));
        Assert.assertArrayEquals(struct2.getLongList("amf").toArray(), ((List<Long>)(r.get("amf"))).toArray());
        Assert.assertArrayEquals(struct2.getStringList("asf").toArray(), ((List<String>)(r.get("asf"))).toArray());
        Assert.assertArrayEquals(struct2.getDateList("adf").stream()
                .map(StructAndRecordConverterTest::getEpochDays)
                .collect(Collectors.toList())
                .toArray(), ((List<Integer>)(r.get("adf"))).toArray());
        Assert.assertArrayEquals(struct2.getTimestampList("atf").stream()
                .map(t -> t.getSeconds()*1000)
                .collect(Collectors.toList())
                .toArray(), ((List<Long>)(r.get("atf"))).toArray());

        // Test struct to record (nested entity)
        GenericRecord c = (GenericRecord) r.get("rf");
        Assert.assertTrue((Boolean) c.get("cbf"));
        Assert.assertEquals(struct1.getLong("cif"), c.get("cif"));
        Assert.assertEquals(struct1.getDouble("cff"), c.get("cff"));
        Assert.assertEquals(struct1.getString("csf"), c.get("csf"));
        Assert.assertEquals(getEpochDays(struct1.getDate("cdf")), c.get("cdf"));
        Assert.assertEquals(struct1.getTimestamp("ctf").getSeconds()*1000, c.get("ctf"));

        // Test struct to record (nested array entity)
        GenericRecord a = ((List<GenericRecord>)r.get("arf")).get(0);
        Assert.assertTrue((Boolean) a.get("cbf"));
        Assert.assertEquals(struct1.getLong("cif"), a.get("cif"));
        Assert.assertEquals(struct1.getDouble("cff"), a.get("cff"));
        Assert.assertEquals(struct1.getString("csf"), a.get("csf"));
        Assert.assertEquals(getEpochDays(struct1.getDate("cdf")), a.get("cdf"));
        Assert.assertEquals(struct1.getTimestamp("ctf").getSeconds()*1000, a.get("ctf"));

        // Test record to struct
        Struct struct3 = RecordToStructConverter.convert(r);
        Assert.assertEquals(struct2.getBoolean("bf"), struct3.getBoolean("bf"));
        Assert.assertEquals(struct2.getLong("if"), struct3.getLong("if"));
        Assert.assertEquals(struct2.getDouble("ff"), struct3.getDouble("ff"), 0);
        Assert.assertEquals(struct2.getString("sf"), struct3.getString("sf"));
        Assert.assertEquals(struct2.isNull("nf"), struct3.isNull("nf"));
        Assert.assertEquals(struct2.getDate("df"), struct3.getDate("df"));
        Assert.assertEquals(struct2.getTimestamp("tf"), struct3.getTimestamp("tf"));

        Assert.assertArrayEquals(struct2.getLongArray("aif"), struct3.getLongArray("aif"));
        Assert.assertArrayEquals(struct2.getStringList("asf").toArray(), struct3.getStringList("asf").toArray());
        Assert.assertArrayEquals(struct2.getDateList("adf").toArray(), struct3.getDateList("adf").toArray());
        Assert.assertArrayEquals(struct2.getLongList("amf").toArray(), struct3.getLongList("amf").toArray());
        Assert.assertArrayEquals(struct2.getTimestampList("atf").toArray(), struct3.getTimestampList("atf").toArray());
    }

    @Test
    public void testConverts() {

    }

    private static int getEpochDays(Date date) {
        DateTime datetime = new DateTime(date.getYear(), date.getMonth(), date.getDayOfMonth(), 0, 0, DateTimeZone.UTC);
        Days days = Days.daysBetween(EPOCHDATETIME, datetime);
        return days.getDays();
    }
}