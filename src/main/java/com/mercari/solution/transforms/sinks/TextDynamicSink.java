/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.transforms.sinks;

import static org.apache.commons.compress.utils.CharsetNames.UTF_8;
import org.apache.beam.sdk.io.FileIO;
import java.io.*;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

public class TextDynamicSink<ElementT> implements FileIO.Sink<ElementT> {

    private final String type;
    private final String header;
    private final RecordFormatter<ElementT> formatter;
    private transient PrintWriter writer;

    private TextDynamicSink(final String type, final String header, final RecordFormatter formatter) {
        this.type = type;
        this.header = header;
        this.formatter = formatter;
    }

    public static <ElementT> TextDynamicSink<ElementT> of(final String type, final RecordFormatter<ElementT> formatter) {
        return new TextDynamicSink(type, null, formatter);
    }

    public static <ElementT> TextDynamicSink<ElementT> of(final String type, final String header, final RecordFormatter<ElementT> formatter) {
        return new TextDynamicSink(type, header, formatter);
    }

    @Override
    public void open(WritableByteChannel channel) throws IOException {
        this.writer = new PrintWriter(
                        new BufferedWriter(
                                new OutputStreamWriter(Channels.newOutputStream(channel), UTF_8)));
        if(this.header != null && this.header.trim().length() > 0) {
            this.writer.println(this.header);
        }
    }

    @Override
    public void write(ElementT element) throws IOException {
        this.writer.println(this.formatter.formatText(element, this.type));
    }

    @Override
    public void flush() throws IOException {
        this.writer.close();
    }

    public interface RecordFormatter<ElementT> extends Serializable {
        String formatText(ElementT element, String type);
    }

}
