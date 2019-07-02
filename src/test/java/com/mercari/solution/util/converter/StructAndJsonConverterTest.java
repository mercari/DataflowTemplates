/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.cloud.spanner.Struct;
import com.mercari.solution.util.DummyDataSupplier;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Test cases for the {@link StructToJsonConverter} class. */
@RunWith(JUnit4.class)
public class StructAndJsonConverterTest {

    @Test
    public void test() {

        Struct struct = DummyDataSupplier.createNestedStruct(false);
        String json = StructToJsonConverter.convert(struct);
        Assert.assertEquals("{\"bf\":false,\"if\":-12,\"ff\":110.005,\"sf\":\"I am a pen\",\"df\":\"2018-10-01\",\"tf\":\"2018-10-01T03:00:00Z\",\"nf\":null,\"lnf\":null,\"dnf\":null,\"tnf\":null,\"rf\":{\"cbf\":true,\"cif\":12,\"cff\":0.005,\"cdf\":\"2018-09-01\",\"ctf\":\"2018-09-01T03:00:00Z\",\"csf\":\"This is a pen\",\"cnf\":\"AAAAAAAAAAAAAAAABfXhAA==\"},\"arf\":[{\"cbf\":true,\"cif\":12,\"cff\":0.005,\"cdf\":\"2018-09-01\",\"ctf\":\"2018-09-01T03:00:00Z\",\"csf\":\"This is a pen\",\"cnf\":\"AAAAAAAAAAAAAAAABfXhAA==\"}],\"asf\":[\"a\",\"b\",\"c\"],\"aif\":[1,2,3],\"adf\":[\"2018-09-01\",\"2018-10-01\"],\"anf\":null,\"amf\":[1,2,3],\"atf\":[\"2018-09-01T03:00:00Z\",\"2018-10-01T03:00:00Z\"]}", json);

    }
}