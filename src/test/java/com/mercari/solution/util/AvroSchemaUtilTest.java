package com.mercari.solution.util;

import org.junit.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AvroSchemaUtilTest {

    Pattern p = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");

    @Test
    public void test() {
        final String str1 = "2019-06-10";
        final Matcher matcher1 = p.matcher(str1);
        if(matcher1.find()) {
            System.out.println(str1.substring(matcher1.start(), matcher1.end()));
        } else {
            System.out.println("no");
        }

        Set<String> check = new HashSet<>();
        //check.add("");
        System.out.println(check.contains(""));
    }
}
