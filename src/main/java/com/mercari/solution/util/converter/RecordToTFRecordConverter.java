/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.protobuf.ByteString;
import com.mercari.solution.util.AvroSchemaUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.tensorflow.example.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converter converts Avro GenericRecord to TFRecord
 */
public class RecordToTFRecordConverter {

    /**
     * Convert Avro {@link com.google.cloud.bigquery.Schema} to TFRecord.
     * (for BigQueryIO.Write.withSchemaFromView)
     *
     * @param record Avro GenericRecord object
     * @return byte array of TFRecord.
     */
    public static byte[] convert(GenericRecord record) {
        return convert(record.getSchema(), record);
    }

    /**
     * Convert Avro format object {@link SchemaAndRecord} to TFRecord.
     *
     * @param schemaAndRecord Avro object from BigQuery export results that contains BigQuery schema info and GenericRecord
     * @return byte array of TFRecord.
     */
    public static byte[] convert(SchemaAndRecord schemaAndRecord) {
        final Schema schema = AvroSchemaUtil.convertSchema(schemaAndRecord.getTableSchema());
        return convert(schema, schemaAndRecord.getRecord());
    }

    /**
     * Convert Avro {@link com.google.cloud.bigquery.Schema} to TFRecord.
     * (for BigQueryIO.Write.withSchemaFromView)
     *
     * @param schema Avro Schema object
     * @param record Avro GenericRecord object
     * @return byte array of TFRecord.
     */
    public static byte[] convert(Schema schema, GenericRecord record) {
        final Features features = Features.newBuilder().putAllFeature(getFeatureMap(null, schema, record)).build();
        final Example example = Example.newBuilder().setFeatures(features).build();
        return example.toByteArray();
    }

    private static Feature getFeature(final Schema schema, Object value) {
        final Feature.Builder builder = Feature.newBuilder();
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return builder.setBytesList(BytesList.newBuilder().addValue(ByteString.copyFrom(value.toString().getBytes()))).build();
            case FIXED:
                return builder.setBytesList(BytesList.newBuilder().addValue(ByteString.copyFrom(((GenericData.Fixed)value).bytes()))).build();
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(((ByteBuffer)value).array()).longValue(), scale);
                    return builder.setFloatList(FloatList.newBuilder().addValue((float)(bigDecimal.doubleValue())).build()).build();
                }
                return builder.setBytesList(BytesList.newBuilder().addValue(ByteString.copyFrom((ByteBuffer)value))).build();
            case FLOAT:
                return builder.setFloatList(FloatList.newBuilder().addValue((Float)value)).build();
            case DOUBLE:
                return builder.setFloatList(FloatList.newBuilder().addValue((float)(double)value)).build();
            case INT:
                return builder.setInt64List(Int64List.newBuilder().addValue((Integer)value)).build();
            case LONG:
                return builder.setInt64List(Int64List.newBuilder().addValue((Long)value)).build();
            case BOOLEAN:
                return builder.setInt64List(Int64List.newBuilder().addValue((Boolean)value ? 1 : 0)).build();
            default:
                throw new IllegalArgumentException(String.format("Not supported schema type %s", schema.getType().name()));
        }
    }

    private static Feature getFeatureArray(final Schema schema, Object value) {
        final Feature.Builder builder = Feature.newBuilder();
        switch (schema.getType()) {
            case ENUM:
            case STRING:
                return builder.setBytesList(BytesList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> s == null ? "" : s)
                                .map(s -> ByteString.copyFrom(s.toString().getBytes()))
                                .collect(Collectors.toList()))).build();
            case FIXED:
                return builder.setBytesList(BytesList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> ByteString.copyFrom(((GenericData.Fixed)s).bytes()))
                                .collect(Collectors.toList()))).build();
            case BYTES:
                if(AvroSchemaUtil.isLogicalTypeDecimal(schema)) {
                    final int scale = AvroSchemaUtil.getLogicalTypeDecimal(schema).getScale();
                    return builder.setFloatList(FloatList.newBuilder().addAllValue(
                            ((List<Object>)value).stream()
                                    .map(s -> BigDecimal.valueOf(new BigInteger(((ByteBuffer)s).array()).longValue(), scale))
                                    .map(s -> (float)s.doubleValue())
                                    .collect(Collectors.toList()))).build();
                }
                return builder.setBytesList(BytesList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> ByteString.copyFrom((ByteBuffer)s))
                                .collect(Collectors.toList()))).build();
            case FLOAT:
                return builder.setFloatList(FloatList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Float)s)
                                .collect(Collectors.toList()))).build();
            case DOUBLE:
                return builder.setFloatList(FloatList.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (float)(double)s)
                                .collect(Collectors.toList()))).build();
            case INT:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Long)s)
                                .collect(Collectors.toList()))).build();
            case LONG:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Long)s)
                                .collect(Collectors.toList()))).build();
            case BOOLEAN:
                return builder.setInt64List(Int64List.newBuilder().addAllValue(
                        ((List<Object>)value).stream()
                                .map(s -> (Boolean)s)
                                .map(s -> s ? 1L : 0L)
                                .collect(Collectors.toList()))).build();
            default:
                throw new IllegalArgumentException(String.format("Not supported schema type %s", schema.getType().name()));
        }
    }

    private static Map<String,Feature> getFeatureMap(final String fieldName, final Schema schema, GenericRecord value) {
        final Map<String,Feature> featureMap = new HashMap<>();
        for(final Schema.Field field : schema.getFields()) {
            final Schema fieldSchema = AvroSchemaUtil.unnestUnion(field.schema());
            final String childFieldName = (fieldName == null ? "" : fieldName + "_") + field.name();
            switch (fieldSchema.getType()) {
                case RECORD:
                    featureMap.putAll(getFeatureMap(childFieldName, fieldSchema, (GenericRecord)value.get(field.name())));
                    break;
                case ARRAY:
                    featureMap.put(childFieldName, getFeatureArray(fieldSchema.getElementType(), value));
                    break;
                case MAP:
                case NULL:
                    break;
                default:
                    featureMap.put(childFieldName, getFeature(fieldSchema, value.get(field.name())));
                    break;
            }
        }
        return featureMap;
    }

}