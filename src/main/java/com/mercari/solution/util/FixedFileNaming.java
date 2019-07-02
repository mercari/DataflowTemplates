/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;

/**
 * Custom FileNaming.
 */
public class FixedFileNaming implements FileIO.Write.FileNaming {

    private final String key;
    private final String type;

    private FixedFileNaming(final String key, final String type) {
        this.key = key;
        this.type = type;
    }

    public String getFilename(final BoundedWindow window,
                              final PaneInfo pane,
                              final int numShards,
                              final int shardIndex,
                              final Compression compression) {

        return String.format("%s.%s", this.key, this.type);
    }

    public static FixedFileNaming of(final String key, final String type) {
        return new FixedFileNaming(key, type);
    }

}
