/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import com.google.common.hash.Hashing;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class DummyGenericRecordGenerator {

    private static final Random random = new Random();
    private static final int NULL_RATE = 20;

    @Test
    public void generateDummyAvroFile() throws Exception {
        final int count = 10;
        final String schemaFilePath = ClassLoader.getSystemResource("it/dummy_schema_avro_spanner.json").getPath();
        final String avroFilePath = "/Users/orfeon/dummy_schema_avro_spanner.avro";

        Schema schema = new Schema.Parser().parse(new File(schemaFilePath));
        List<GenericRecord> records = IntStream.range(0, count).boxed().map(i -> generate(schema)).collect(Collectors.toList());
        try (DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            writer.create(schema, new File(avroFilePath));
            records.stream().forEach(record -> {
                try {
                    writer.append(record);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    public static List<GenericRecord> generate(String schemaFilePath, int count) throws IOException {
        Schema schema = new Schema.Parser().parse(new File(schemaFilePath));
        return generate(schema, count);
    }

    public static List<GenericRecord> generate(Schema schema, int count) {
        return IntStream.range(0, count).boxed()
                .map(i -> generate(schema))
                .collect(Collectors.toList());
    }

    public static GenericRecord generate(Schema schema) {
        final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        for(final Schema.Field field : schema.getFields()) {
            builder.set(field.name(), randomValue(field.schema()));
        }
        return builder.build();
    }

    private static boolean randomNull(boolean isNullable, int randomRate) {
        return isNullable && random.nextInt(100) < randomRate;
    }

    private static String randomString(boolean nullable) {
        return Hashing.sha1().hashLong(random.nextLong()).toString();
    }

    private static List<String> randomStringArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomString(nullable))
                .collect(Collectors.toList());
    }

    private static GenericData.Fixed randomFixed(boolean nullable, Schema schema) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        final byte[] randomBytes = Hashing.sha512().hashLong(random.nextLong()).asBytes();
        //return ByteBuffer.wrap(randomBytes);
        return new GenericData.Fixed(schema, randomBytes);
    }

    private static List<GenericData.Fixed> randomFixedArray(boolean nullable, Schema schema) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomFixed(nullable, schema))
                .collect(Collectors.toList());
    }

    private static ByteBuffer randomBytes(boolean nullable) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        final byte[] randomBytes = Hashing.sha512().hashLong(random.nextLong()).asBytes();
        return ByteBuffer.wrap(randomBytes);
    }

    private static List<ByteBuffer> randomBytesArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomBytes(nullable))
                .collect(Collectors.toList());
    }

    private static ByteBuffer randomDecimal(boolean nullable, int precision, int scale) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for(int i=precision; i>0; i--) {
            if(i == scale) {
                sb.append(".");
            }
            sb.append("1");
        }
        BigDecimal decimal = new BigDecimal(sb.toString());
        return ByteBuffer.wrap(decimal.toBigInteger().toByteArray());
    }

    private static List<ByteBuffer> randomDecimalArray(boolean nullable, int precision, int scale) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomDecimal(nullable, precision, scale))
                .collect(Collectors.toList());

    }

    private static GenericData.Fixed randomFixedDecimal(boolean nullable, int precision, int scale, Schema schema) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for(int i=precision; i>0; i--) {
            if(i == scale) {
                sb.append(".");
            }
            sb.append("1");
        }
        BigDecimal decimal = new BigDecimal(sb.toString());
        return new GenericData.Fixed(schema, decimal.toBigInteger().toByteArray());
    }

    private static List<GenericData.Fixed> randomFixedDecimalArray(boolean nullable, int precision, int scale, Schema schema) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomFixedDecimal(nullable, precision, scale, schema))
                .collect(Collectors.toList());
    }

    private static Integer randomDate(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextInt(365 * 10) + 365 * 30;
    }

    private static List<Integer> randomDateArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomDate(nullable))
                .collect(Collectors.toList());
    }

    private static Integer randomTimeMilliSecond(boolean nullable) {
        int intValue = random.nextInt(86399999);
        return randomNull(nullable, NULL_RATE) ? null : intValue;

    }
    private static List<Integer> randomTimeMilliSecondArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomTimeMilliSecond(nullable))
                .collect(Collectors.toList());
    }

    private static Long randomTimeMicroSecond(boolean nullable) {
        long longValue = Math.abs(random.nextLong()) % 86400000000L;
        return randomNull(nullable, NULL_RATE) ? null : longValue;
    }
    private static List<Long> randomTimeMicroSecondArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomTimeMicroSecond(nullable))
                .collect(Collectors.toList());
    }

    private static Long randomTimestampMicros(boolean nullable) {
        long longValue = random.nextLong() % 86399999999L;
        return randomNull(nullable, NULL_RATE) ? null : longValue;
    }

    private static List<Long> randomTimestampMicrosArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomTimestampMicros(nullable))
                .collect(Collectors.toList());
    }

    private static Long randomTimestampMillis(boolean nullable) {
        long longValue = random.nextLong() % 86399999L;
        return randomNull(nullable, NULL_RATE) ? null : longValue;
    }
    private static List<Long> randomTimestampMillisArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomTimestampMillis(nullable))
                .collect(Collectors.toList());
    }

    private static Integer randomInt(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextInt();
    }
    private static List<Integer> randomIntArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomInt(nullable))
                .collect(Collectors.toList());
    }

    private static Long randomLong(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextLong();
    }
    private static List<Long> randomLongArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomLong(nullable))
                .collect(Collectors.toList());
    }

    private static Float randomFloat(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextFloat();
    }
    private static List<Float> randomFloatArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomFloat(nullable))
                .collect(Collectors.toList());
    }

    private static Double randomDouble(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextDouble();
    }
    private static List<Double> randomDoubleArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomDouble(nullable))
                .collect(Collectors.toList());
    }

    private static Boolean randomBoolean(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : random.nextBoolean();
    }
    private static List<Boolean> randomBooleanArray(boolean nullable) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomBoolean(nullable))
                .collect(Collectors.toList());
    }

    private static GenericData.EnumSymbol randomEnum(boolean nullable, Schema schema) {

        return randomNull(nullable, NULL_RATE) ? null : new GenericData.EnumSymbol(schema, schema.getEnumSymbols().get(random.nextInt(schema.getEnumSymbols().size())));
    }
    private static List<GenericData.EnumSymbol> randomEnumArray(boolean nullable, Schema schema) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomEnum(nullable, schema))
                .collect(Collectors.toList());
    }

    private static Map<String, Object> randomMap(boolean nullable, Schema schema) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        Map<String, Object> map = new HashMap<>();
        for(int i = 0; i < 4; i++) {
            map.put(randomString(false), randomValue(schema));
        }
        return map;
    }
    private static List<Map<String,Object>> randomMapArray(boolean nullable, Schema schema) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomMap(nullable, schema))
                .collect(Collectors.toList());
    }

    private static GenericRecord randomRecord(boolean nullable, Schema schema) {
        if(randomNull(nullable, NULL_RATE)) {
            return null;
        }
        final GenericRecordBuilder childBuilder = new GenericRecordBuilder(schema);
        for(final Schema.Field childField : schema.getFields()) {
            childBuilder.set(childField.name(), randomValue(childField.schema()));
        }
        return childBuilder.build();
    }

    private static List<GenericRecord> randomRecordArray(boolean nullable, Schema schema) {
        return randomNull(nullable, NULL_RATE) ? null : IntStream.range(0, 4).boxed()
                .map(i -> randomRecord(nullable, schema))
                .collect(Collectors.toList());
    }

    private static Object randomValue(Schema schema) {
        return randomValue(false, schema, false);
    }

    private static Object randomValue(boolean nullable, Schema schema, boolean isarray) {
        switch (schema.getType()) {
            case STRING:
                return isarray ? randomStringArray(nullable) : randomString(nullable);
            case FIXED:
            case BYTES:
                final Map<String,Object> props = schema.getObjectProps();
                final int scale = props.containsKey("scale") ? Integer.valueOf(props.get("scale").toString()) : 0;
                final int precision = props.containsKey("precision") ? Integer.valueOf(props.get("precision").toString()) : 0;
                if(LogicalTypes.decimal(precision, scale).equals(schema.getLogicalType())) {
                    if(Schema.Type.FIXED.equals(schema.getType())) {
                        return isarray ? randomFixedDecimalArray(nullable, precision, scale, schema) : randomFixedDecimal(nullable, precision, scale, schema);
                    }
                    return isarray ? randomDecimalArray(nullable, precision, scale) : randomDecimal(nullable, precision, scale);
                }
                if(Schema.Type.FIXED.equals(schema.getType())) {
                    return isarray ? randomFixedArray(nullable, schema) : randomFixed(nullable, schema);
                }
                return isarray ? randomBytesArray(nullable) : randomBytes(nullable);
            case INT:
                if(LogicalTypes.date().equals(schema.getLogicalType())) {
                    return isarray ? randomDateArray(nullable) : randomDate(nullable);
                } else if(LogicalTypes.timeMillis().equals(schema.getLogicalType())) {
                    return isarray ? randomTimeMilliSecondArray(nullable) : randomTimeMilliSecond(nullable);
                } else {
                    return isarray ? randomIntArray(nullable) : randomInt(nullable);
                }
            case LONG:
                if(LogicalTypes.timestampMillis().equals(schema.getLogicalType())) {
                    return isarray ? randomTimestampMillisArray(nullable) : randomTimestampMillis(nullable);
                } else if(LogicalTypes.timestampMicros().equals(schema.getLogicalType())) {
                    return isarray ? randomTimestampMicrosArray(nullable) : randomTimestampMicros(nullable);
                } else if(LogicalTypes.timeMicros().equals(schema.getLogicalType())) {
                    return isarray ? randomTimeMicroSecondArray(nullable) : randomTimeMicroSecond(nullable);
                } else {
                    return isarray ? randomLongArray(nullable) : randomLong(nullable);
                }
            case FLOAT:
                return isarray ? randomFloatArray(nullable) : randomFloat(nullable);
            case DOUBLE:
                return isarray ? randomDoubleArray(nullable) : randomDouble(nullable);
            case BOOLEAN:
                return isarray ? randomBooleanArray(nullable) : randomBoolean(nullable);
            case RECORD:
                return isarray ? randomRecordArray(nullable, schema) : randomRecord(nullable, schema);
            case UNION:
                for(final Schema childSchema : schema.getTypes()) {
                    if(Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    return randomValue(true, childSchema, isarray);
                }
                return null;
            case ARRAY:
                return randomValue(false, schema.getElementType(), true);
            case NULL:
                return null;
            case ENUM:
                return isarray ? randomEnumArray(nullable, schema.getElementType()) : randomEnum(nullable, schema);
            case MAP:
                return isarray ? randomMapArray(nullable, schema.getValueType()) : randomMap(nullable, schema.getValueType());
            default:
                return null;
        }
    }

}