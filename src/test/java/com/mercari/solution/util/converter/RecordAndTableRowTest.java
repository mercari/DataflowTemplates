/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util.converter;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.mercari.solution.util.AvroSchemaUtil;
import com.mercari.solution.util.DummyGenericRecordGenerator;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.Test;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RecordAndTableRowTest {

    @Test
    public void testNotNull() throws Exception {
        final String schemaFilePath = ClassLoader.getSystemResource("avro/dummy_schema_notnull.json").getPath();
        final List<GenericRecord> records = DummyGenericRecordGenerator.generate(schemaFilePath, 10);
        for(final GenericRecord record : records) {
            final TableRow row = RecordToTableRowConverter.convert(record);
            System.out.println(record);
            System.out.println(row);
            for(final Schema.Field field : record.getSchema().getFields()) {
                assertRecordAndTableRow(field, field.schema(), record, row);
            }
        }
    }

    @Test
    public void testNullable() throws Exception {
        final String schemaFilePath = ClassLoader.getSystemResource("avro/dummy_schema_nullable.json").getPath();
        final List<GenericRecord> records = DummyGenericRecordGenerator.generate(schemaFilePath, 10);
        for(final GenericRecord record : records) {
            final TableRow row = RecordToTableRowConverter.convert(record);
            System.out.println(record);
            System.out.println(row);
            for(final Schema.Field field : record.getSchema().getFields()) {
                assertRecordAndTableRow(field, field.schema(), record, row);
            }
        }
    }

    @Test
    public void testMixNullable() throws Exception {
        final String schemaFilePath = ClassLoader.getSystemResource("avro/dummy_schema.json").getPath();
        final List<GenericRecord> records = DummyGenericRecordGenerator.generate(schemaFilePath, 10);
        final TableSchema ts = RecordToTableRowConverter.convertTableSchema(records.get(0).getSchema());
        System.out.println(ts);
        for(final GenericRecord record : records) {
            final TableRow row = RecordToTableRowConverter.convert(record);
            System.out.println(record);
            System.out.println(row);
            for(final Schema.Field field : record.getSchema().getFields()) {
                assertRecordAndTableRow(field, field.schema(), record, row);
            }
        }
    }

    @Test
    public void testSchema() throws Exception {
        final String schemaFilePath = ClassLoader.getSystemResource("avro/dummy_schema.json").getPath();
        Schema schema = new Schema.Parser().parse(new File(schemaFilePath));
        TableSchema ts = RecordToTableRowConverter.convertTableSchema(schema);
        System.out.println(ts);
    }

    private void assertRecordAndTableRow(final Schema.Field field, final Schema type, final GenericRecord record, final TableRow row) {
        if(record == null && row == null) {
            return;
        }
        assert record != null && row != null;
        if(record.get(field.name()) == null && row.get(field.name()) == null) {
            return;
        }
        switch (type.getType()) {
            case ENUM:
            case STRING:
                assert record.get(field.name()).toString().equals(row.get(field.name()));
                break;
            case INT:
                final Long intValue = new Long((Integer)record.get(field.name()));
                if(LogicalTypes.date().equals(type.getLogicalType())) {
                    final LocalDate localDate = LocalDate.ofEpochDay(intValue);
                    assert localDate.format(DateTimeFormatter.ISO_LOCAL_DATE).equals(row.get(field.name()));
                } else if(LogicalTypes.timeMillis().equals(type.getLogicalType())) {
                    final LocalTime localTime = LocalTime.ofNanoOfDay(intValue * 1000 * 1000);
                    assert localTime.format(DateTimeFormatter.ISO_LOCAL_TIME).equals(row.get(field.name()));
                } else {
                    assert record.get(field.name()).equals(row.get(field.name()));
                }
                break;
            case LONG:
                final Long longValue = (Long)record.get(field.name());
                if(LogicalTypes.timestampMillis().equals(type.getLogicalType())
                        || LogicalTypes.timestampMicros().equals(type.getLogicalType())) {
                    final Long seconds = type.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue / 1000000 : longValue / 1000;
                    assert seconds.equals(row.get(field.name()));
                } else if(LogicalTypes.timeMicros().equals(type.getLogicalType())) {
                    final LocalTime localTime = LocalTime.ofNanoOfDay(longValue * 1000);
                    assert localTime.format(DateTimeFormatter.ISO_LOCAL_TIME).equals(row.get(field.name()));
                } else {
                    assert longValue.equals(row.get(field.name()));
                }
                break;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                assert record.get(field.name()).equals(row.get(field.name()));
                break;
            case BYTES:
                final int precision = type.getObjectProp("precision") != null ? Integer.valueOf(type.getObjectProp("precision").toString()) : 0;
                final int scale = type.getObjectProp("scale") != null ? Integer.valueOf(type.getObjectProp("scale").toString()) : 0;
                final ByteBuffer bytes = (ByteBuffer)record.get(field.name());
                if(LogicalTypes.decimal(precision, scale).equals(type.getLogicalType())) {
                    final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale);
                    assert bigDecimal.equals(row.get(field.name()));
                } else {
                    assert bytes.equals(row.get(field.name()));
                }
                break;
            case MAP:
                final Map map = (Map)record.get(field.name());
                for(final TableRow childMapRow : (List<TableRow>)row.get(field.name())) {
                    if(childMapRow.get("value") == null) {
                        assert map.get(new Utf8(childMapRow.get("key").toString())) == null;
                    } else if(map.get(new Utf8(childMapRow.get("key").toString())) instanceof Utf8) {
                        assert map.get(new Utf8(childMapRow.get("key").toString())).toString().equals(childMapRow.get("value"));
                    } else {
                        if(Schema.Type.UNION.equals(type.getValueType().getType())) {
                            for(Schema childSchema : type.getValueType().getTypes()) {
                                if(Schema.Type.UNION.equals(childSchema.getType())) {
                                    assert childMapRow.get("value") == null;
                                    continue;
                                }
                                if(!map.containsKey(new Utf8(childMapRow.get("key").toString()))) {
                                    continue;
                                }
                                switch (childSchema.getType()) {
                                    case INT:
                                        if(LogicalTypes.date().equals(childSchema.getLogicalType())) {
                                            final LocalDate localDate = LocalDate.ofEpochDay((Integer)map.get(new Utf8(childMapRow.get("key").toString())));
                                            assert localDate.format(DateTimeFormatter.ISO_LOCAL_DATE).equals(childMapRow.get("value"));
                                            break;
                                        }
                                }
                            }
                        } else {
                            //System.out.println(map.get(new Utf8(childMapRow.get("key").toString())));
                            //System.out.println(childMapRow.get("value"));
                            //assert map.get(new Utf8(childMapRow.get("key").toString())).equals(childMapRow.get("value"));
                        }
                    }
                }
                break;
            case FIXED:
                final GenericData.Fixed fixed = (GenericData.Fixed)record.get(field.name());
                if(AvroSchemaUtil.isLogicalTypeDecimal(field.schema())) {
                    final int scaleFixed = AvroSchemaUtil.getLogicalTypeDecimal(field.schema()).getScale();
                    final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(fixed.bytes()).longValue(), scaleFixed);
                    assert bigDecimal.equals(row.get(field.name()));
                    break;
                }
                final ByteBuffer bf = ByteBuffer.wrap(fixed.bytes());
                assert bf.equals(row.get(field.name()));
                break;
            case RECORD:
                final GenericRecord childRecord = (GenericRecord) record.get(field.name());
                for(final Schema.Field childField : childRecord.getSchema().getFields()) {
                    assertRecordAndTableRow(childField, childField.schema(), childRecord, (TableRow)row.get(field.name()));
                }
                break;
            case ARRAY:
                assertArrayRecordAndTableRow(type.getElementType(), (List)record.get(field.name()), (List)row.get(field.name()));
                break;
            case UNION:
                for(final Schema childSchema : type.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    assertRecordAndTableRow(field, childSchema, record, row);
                }
                break;
            default:
                break;
        }

    }

    private void assertArrayRecordAndTableRow(final Schema scheme, final List recordArray_, final List rowArray) {
        if(recordArray_ == null && rowArray == null) {
            return;
        }
        final List recordArray = (List)(recordArray_.stream().filter(v -> v != null).collect(Collectors.toList()));
        assert recordArray != null && rowArray != null;
        assert recordArray.size() == rowArray.size();
        final int size = recordArray.size();
        switch (scheme.getType()) {
            case ENUM:
            case STRING:
                for(int i=0; i<size; i++) {
                    if(recordArray.get(i) == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    assert recordArray.get(i).toString().equals(rowArray.get(i));
                }
                break;
            case INT:
                for(int i=0; i<size; i++) {
                    final Integer intValue = (Integer) recordArray.get(i);
                    if(intValue == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    final Long intLongValue = new Long(intValue);
                    if (LogicalTypes.date().equals(scheme.getLogicalType())) {
                        final LocalDate localDate = LocalDate.ofEpochDay(intLongValue);
                        assert localDate.format(DateTimeFormatter.ISO_LOCAL_DATE).equals(rowArray.get(i));
                    } else if(LogicalTypes.timeMillis().equals(scheme.getLogicalType())) {
                        final LocalTime localTime = LocalTime.ofNanoOfDay(intLongValue * 1000 * 1000);
                        assert localTime.format(DateTimeFormatter.ISO_LOCAL_TIME).equals(rowArray.get(i));
                    } else {
                        assert intValue.equals(rowArray.get(i));
                    }
                }
                break;
            case LONG:
                for(int i=0; i<size; i++) {
                    final Long longValue = (Long) recordArray.get(i);
                    if(longValue == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    if (LogicalTypes.timestampMillis().equals(scheme.getLogicalType())
                            || LogicalTypes.timestampMicros().equals(scheme.getLogicalType())) {
                        final Long seconds = scheme.getLogicalType().equals(LogicalTypes.timestampMicros()) ? longValue / 1000000 : longValue / 1000;
                        assert seconds.equals(rowArray.get(i));
                    } else if(LogicalTypes.timeMicros().equals(scheme.getLogicalType())) {
                        final LocalTime localTime = LocalTime.ofNanoOfDay(longValue * 1000);
                        assert localTime.format(DateTimeFormatter.ISO_LOCAL_TIME).equals(rowArray.get(i));
                    } else {
                        assert longValue.equals(rowArray.get(i));
                    }
                }
                break;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                for(int i=0; i<size; i++) {
                    if(recordArray.get(i) == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    assert recordArray.get(i).equals(rowArray.get(i));
                }
                break;
            case BYTES:
                for(int i=0; i<size; i++) {
                    final ByteBuffer bytes = (ByteBuffer) recordArray.get(i);
                    if(bytes == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    final int precision = scheme.getObjectProp("precision") != null ? Integer.valueOf(scheme.getObjectProp("precision").toString()) : 0;
                    final int scale = scheme.getObjectProp("scale") != null ? Integer.valueOf(scheme.getObjectProp("scale").toString()) : 0;
                    if (LogicalTypes.decimal(precision, scale).equals(scheme.getLogicalType())) {
                        final BigDecimal bigDecimal = BigDecimal.valueOf(new BigInteger(bytes.array()).longValue(), scale);
                        assert bigDecimal.equals(rowArray.get(i));
                    } else {
                        assert bytes.equals(rowArray.get(i));
                    }
                }
                break;
            case MAP:
                for(int i=0; i<size; i++) {
                    final Map map = (Map) recordArray.get(i);
                    if(map == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    for (final TableRow childMapRow : (List<TableRow>) rowArray.get(i)) {
                        if (map.get(new Utf8(childMapRow.get("key").toString())) instanceof Utf8) {
                            assert map.get(new Utf8(childMapRow.get("key").toString())).toString().equals(childMapRow.get("value"));
                        } else {
                            assert map.get(new Utf8(childMapRow.get("key").toString())).equals(childMapRow.get("value"));
                        }
                    }
                }
                break;
            case FIXED:
                for(int i=0; i<size; i++) {
                    final GenericData.Fixed fixed = (GenericData.Fixed) recordArray.get(i);
                    if(fixed == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    assert fixed.bytes().equals(rowArray.get(i));
                }
                break;
            case RECORD:
                for(int i=0; i<size; i++) {
                    final GenericRecord childRecord = (GenericRecord) recordArray.get(i);
                    if(childRecord == null) {
                        assert rowArray.get(i) == null;
                        continue;
                    }
                    for(final Schema.Field childField : childRecord.getSchema().getFields()) {
                        assertRecordAndTableRow(childField, childField.schema(), childRecord, (TableRow) rowArray.get(i));
                    }
                }
                break;
            case ARRAY:
                break;
            case UNION:
                for(final Schema childSchema : scheme.getTypes()) {
                    if (Schema.Type.NULL.equals(childSchema.getType())) {
                        continue;
                    }
                    assertArrayRecordAndTableRow(childSchema, recordArray, rowArray);
                }
                break;
            default:
                break;
        }

    }

}