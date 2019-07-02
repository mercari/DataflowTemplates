/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import com.google.cloud.spanner.Struct;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Map;

/** Test cases for the {@link StructUtil} class. */
@RunWith(JUnit4.class)
public class StructUtilTest {

    @Test
    public void test() {
        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        Assert.assertEquals(struct1.getString("csf"), StructUtil.getFieldValue("csf", struct1));
        Assert.assertEquals(struct1.getBoolean("cbf"), StructUtil.getFieldValue("cbf", struct1));
        Assert.assertEquals(struct1.getLong("cif"), StructUtil.getFieldValue("cif", struct1));
        Assert.assertEquals(struct1.getDouble("cff"), StructUtil.getFieldValue("cff", struct1));
        Assert.assertEquals(struct1.getDate("cdf"), StructUtil.getFieldValue("cdf", struct1));
        Assert.assertEquals(struct1.getTimestamp("ctf"), StructUtil.getFieldValue("ctf", struct1));

        Struct struct2 = DummyDataSupplier.createNestedStruct(false);
        Assert.assertEquals(struct2.getString("sf"), StructUtil.getFieldValue("sf", struct2));
        Assert.assertEquals(struct2.getBoolean("bf"), StructUtil.getFieldValue("bf", struct2));
        Assert.assertEquals(struct2.getLong("if"), StructUtil.getFieldValue("if", struct2));
        Assert.assertEquals(struct2.getDouble("ff"), StructUtil.getFieldValue("ff", struct2));
        Assert.assertEquals(struct2.getDate("df"), StructUtil.getFieldValue("df", struct2));
        Assert.assertEquals(struct2.getTimestamp("tf"), StructUtil.getFieldValue("tf", struct2));
        Assert.assertNull(StructUtil.getFieldValue("nf", struct2));
        Assert.assertNull(StructUtil.getFieldValue("lnf", struct2));
        Assert.assertNull(StructUtil.getFieldValue("dnf", struct2));
        Assert.assertNull(StructUtil.getFieldValue("tnf", struct2));

        Map<String,Object> map = (Map<String,Object>)StructUtil.getFieldValue("rf", struct2);
        Assert.assertEquals(struct1.getString("csf"), map.get("csf"));

        Assert.assertEquals(struct2.getStringList("asf"), StructUtil.getFieldValue("asf", struct2));
        Assert.assertEquals(struct2.getLongList("aif"), StructUtil.getFieldValue("aif", struct2));
        Assert.assertEquals(struct2.getDateList("adf"), StructUtil.getFieldValue("adf", struct2));
        Assert.assertTrue(struct2.isNull("anf"));
        Assert.assertNull(StructUtil.getFieldValue("anf", struct2));
        Assert.assertEquals(struct2.getLongList("amf"), StructUtil.getFieldValue("amf", struct2));
        Assert.assertEquals(struct2.getTimestampList("atf"), StructUtil.getFieldValue("atf", struct2));
    }

}