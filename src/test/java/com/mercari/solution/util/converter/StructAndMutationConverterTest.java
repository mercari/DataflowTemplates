/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.mercari.solution.util.DummyDataSupplier;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;
import java.util.Map;

/** Test cases for the {@link StructToMutationConverter} and {@link MutationToStructConverter} class. */
@RunWith(JUnit4.class)
public class StructAndMutationConverterTest {

    @Test
    public void testMutationConvert() {

        // Test flat struct to mutation
        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        Mutation mutation1 = StructToMutationConverter.convert(struct1, "mytable1", Mutation.Op.valueOf("INSERT"));
        Assert.assertEquals("mytable1", mutation1.getTable());
        Map<String, Value> map1 = mutation1.asMap();
        Assert.assertEquals(struct1.getString("csf"), map1.get("csf").getString());
        Assert.assertEquals(struct1.getBoolean("cbf"), map1.get("cbf").getBool());
        Assert.assertEquals(struct1.getLong("cif"), map1.get("cif").getInt64());
        Assert.assertEquals(struct1.getDouble("cff"), map1.get("cff").getFloat64(), 0);
        Assert.assertEquals(struct1.getDate("cdf"), map1.get("cdf").getDate());
        Assert.assertEquals(struct1.getTimestamp("ctf"), map1.get("ctf").getTimestamp());

        // Test nested struct to mutation
        Struct struct2 = DummyDataSupplier.createNestedStruct(false);
        Mutation mutation2 = StructToMutationConverter.convert(struct2, "mytable2", Mutation.Op.valueOf("INSERT"));
        Assert.assertEquals("mytable2", mutation2.getTable());
        Map<String, Value> map2 = mutation2.asMap();
        Assert.assertEquals(struct2.getString("sf"), map2.get("sf").getString());
        Assert.assertEquals(struct2.getBoolean("bf"), map2.get("bf").getBool());
        Assert.assertEquals(struct2.getLong("if"), map2.get("if").getInt64());
        Assert.assertEquals(struct2.getDouble("ff"), map2.get("ff").getFloat64(), 0);
        Assert.assertEquals(struct2.getDate("df"), map2.get("df").getDate());
        Assert.assertEquals(struct2.getTimestamp("tf"), map2.get("tf").getTimestamp());
        Assert.assertEquals(struct2.getStringList("asf"), map2.get("asf").getStringArray());
        Assert.assertEquals(struct2.getLongList("aif"), map2.get("aif").getInt64Array());
        Assert.assertEquals(struct2.getDateList("adf"), map2.get("adf").getDateArray());
        Assert.assertEquals(struct2.getTimestampList("atf"), map2.get("atf").getTimestampArray());
        Assert.assertTrue(map2.get("nf").isNull());
        Assert.assertTrue(map2.get("lnf").isNull());
        Assert.assertTrue(map2.get("dnf").isNull());
        Assert.assertTrue(map2.get("tnf").isNull());
        Assert.assertTrue(map2.get("anf").isNull());

        // Test mutation to struct
        Struct struct3 = MutationToStructConverter.convert(mutation2);
        Assert.assertEquals(struct2.getString("sf"), struct3.getString("sf"));
        Assert.assertEquals(struct2.getBoolean("bf"), struct3.getBoolean("bf"));
        Assert.assertEquals(struct2.getLong("if"), struct3.getLong("if"));
        Assert.assertEquals(struct2.getDouble("ff"), struct3.getDouble("ff"), 0);
        Assert.assertEquals(struct2.getDate("df"), struct3.getDate("df"));
        Assert.assertEquals(struct2.getTimestamp("tf"), struct3.getTimestamp("tf"));
        Assert.assertEquals(struct2.getStringList("asf"), struct3.getStringList("asf"));
        Assert.assertEquals(struct2.getLongList("aif"), struct3.getLongList("aif"));
        Assert.assertEquals(struct2.getDateList("adf"), struct3.getDateList("adf"));
        Assert.assertEquals(struct2.getTimestampList("atf"), struct3.getTimestampList("atf"));
        Assert.assertTrue(struct3.isNull("nf"));
        Assert.assertTrue(struct3.isNull("lnf"));
        Assert.assertTrue(struct3.isNull("dnf"));
        Assert.assertTrue(struct3.isNull("tnf"));
        Assert.assertTrue(struct3.isNull("anf"));

    }

    @Test
    public void testMutationGroupConvert() {

        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        Struct struct2 = DummyDataSupplier.createNestedStruct(false);
        Mutation mutation1 = StructToMutationConverter.convert(struct1, "mytable1", Mutation.Op.valueOf("INSERT"));
        Mutation mutation2 = StructToMutationConverter.convert(struct2, "mytable2", Mutation.Op.valueOf("INSERT"));
        MutationGroup mutationGroup = MutationGroup.create(mutation1, mutation2);
        List<Struct> structList = MutationToStructConverter.convert(mutationGroup);
        Assert.assertEquals(struct1, structList.get(1));
        //Assert.assertEquals(struct2, structList.get(0)); // NOT SUPPORT NESTED ATTRIBUTE.

    }
}