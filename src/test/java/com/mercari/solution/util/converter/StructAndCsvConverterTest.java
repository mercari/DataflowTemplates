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

import java.io.IOException;

/** Test case for the {@link StructToCsvConverter} class. */
@RunWith(JUnit4.class)
public class StructAndCsvConverterTest {

    @Test
    public void test() throws IOException {

        Struct struct1 = DummyDataSupplier.createSimpleStruct();
        String csvLine1 = StructToCsvConverter.convert(struct1);
        Assert.assertEquals("true,12,0.005,2018-09-01,2018-09-01T03:00:00Z,This is a pen,AAAAAAAAAAAAAAAABfXhAA==", csvLine1);

        Struct struct2 = DummyDataSupplier.createNestedStruct(false);
        String csvLine2 = StructToCsvConverter.convert(struct2);
        Assert.assertEquals("false,-12,110.005,I am a pen,2018-10-01,2018-10-01T03:00:00Z,,,,,\"{cif=12, cff=0.005, cdf=2018-09-01, ctf=2018-09-01T03:00:00Z, cbf=true, csf=This is a pen, cnf=AAAAAAAAAAAAAAAABfXhAA==}\",\"[{cif=12, cff=0.005, cdf=2018-09-01, ctf=2018-09-01T03:00:00Z, cbf=true, csf=This is a pen, cnf=AAAAAAAAAAAAAAAABfXhAA==}]\",\"[a, b, c]\",\"[1, 2, 3]\",\"[2018-09-01, 2018-10-01]\",,\"[1, 2, 3]\",\"[2018-09-01T03:00:00Z, 2018-10-01T03:00:00Z]\"", csvLine2);

        Struct struct3 = DummyDataSupplier.createNestedStruct(true);
        String csvLine3 = StructToCsvConverter.convert(struct3);
        Assert.assertEquals("false,-12,110.005,I am a pen,2018-10-01,2018-10-01T03:00:00Z,,,,,\"{cif=12, cff=0.005, cdf=2018-09-01, ctf=2018-09-01T03:00:00Z, cbf=true, csf=This is a pen, cnf=AAAAAAAAAAAAAAAABfXhAA==}\",\"[{cif=12, cff=0.005, cdf=2018-09-01, ctf=2018-09-01T03:00:00Z, cbf=true, csf=This is a pen, cnf=AAAAAAAAAAAAAAAABfXhAA==}]\",\"[a, b, c]\",\"[1, 2, 3]\",\"[2018-09-01, 2018-10-01]\",,\"[null, 2, 3]\",\"[2018-09-01T03:00:00Z, 2018-10-01T03:00:00Z]\"", csvLine3);

    }
}