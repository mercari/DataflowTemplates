/*
 * Copyright (c) Mercari, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
package com.mercari.solution.util;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;

/**
 * Utility functions to handle Cloud Storage Objects
 */
public class StorageUtil {

    private StorageUtil() {}

    public static void createText(final String gcsPath, final String text) {
        createBlob(gcsPath, text.getBytes(), "text/plain");
    }

    public static void createBlob(final String gcsPath, final byte[] bytes, final String contentType) {
        final String[] paths = gcsPath.replaceAll("gs://", "").split("/", 2);
        final BlobId blobId = BlobId.of(paths[0], paths[1]);
        StorageOptions.getDefaultInstance().getService()
                .create(BlobInfo.newBuilder(blobId).setContentType(contentType).build(), bytes);
    }

    public static String removeDirSuffix(String output) {
        final boolean isgcs = output.startsWith("gs://");
        final String[] paths = output.replaceAll("gs://", "").split("/", -1);
        final StringBuilder sb = new StringBuilder(isgcs ? "gs://" : "");
        final int end = Math.max(paths.length-1, 1);
        for(int i=0; i<end; i++) {
            sb.append(paths[i]);
            sb.append("/");
        }
        return sb.toString();
    }

    public static String addFilePrefix(String output, String prefix) {
        final String[] paths = output.replaceAll("gs://", "").split("/", -1);
        if(paths.length > 1) {
            return paths[paths.length-1] + prefix;
        }
        return prefix;
    }

}
