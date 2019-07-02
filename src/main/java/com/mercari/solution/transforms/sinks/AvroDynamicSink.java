/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.sinks;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.FileIO;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class AvroDynamicSink<ElementT> implements FileIO.Sink<ElementT> {

    private final String jsonSchema;
    private final RecordFormatter<ElementT> formatter;
    private transient Schema schema;
    private transient DataFileWriter<GenericRecord> writer;

    private AvroDynamicSink(final String jsonSchema, final RecordFormatter formatter) {
        this.jsonSchema = jsonSchema;
        this.formatter = formatter;
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.schema = new Schema.Parser().parse(this.jsonSchema);
        this.writer = new DataFileWriter<>(new GenericDatumWriter<>(this.schema));
        this.writer.setCodec(CodecFactory.snappyCodec());
        this.writer.create(this.schema, Channels.newOutputStream(channel));
    }

    @Override
    public void write(ElementT element) throws IOException {
        this.writer.append(this.formatter.formatRecord(element, schema));
    }

    @Override
    public void flush() throws IOException {
        this.writer.flush();
    }

    public static <ElementT> AvroDynamicSink<ElementT> of(
            final String schemaJson,
            final RecordFormatter<ElementT> formatter) {

        return new AvroDynamicSink(schemaJson, formatter);
    }

    public static <ElementT> AvroDynamicSink<ElementT> of(
            final Schema schema,
            final RecordFormatter<ElementT> formatter) {

        return new AvroDynamicSink(schema.toString(), formatter);
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        GenericRecord formatRecord(ElementT element, Schema schema);
    }

}
