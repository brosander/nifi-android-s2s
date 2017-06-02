/*
 * Copyright 2017 Hortonworks, Inc.
 * All rights reserved.
 *
 *   Hortonworks, Inc. licenses this file to you under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * See the associated NOTICE file for additional information regarding copyright ownership.
 */

package com.hortonworks.hdf.android.sitetosite.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class IOUtils {
    private static final int MAX_LONG_LEN = String.valueOf(Long.MAX_VALUE).length();

    /**
     * Reads the contents of an input stream into a long value
     *
     * @param inputStream the input stream
     * @return the value
     * @throws IOException if there are problems reading the stream
     */
    public static long readInputStreamAndParseAsLong(InputStream inputStream) throws IOException {
        byte[] buf = new byte[MAX_LONG_LEN];
        int read;
        int offset = 0;
        while((read = (inputStream.read(buf, offset, MAX_LONG_LEN - offset))) != -1 && offset < buf.length) {
            offset += read;
        }
        return Long.valueOf(new String(buf, 0, offset));
    }

    public static byte[] readInputStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        copy(inputStream, outputStream);
        return outputStream.toByteArray();
    }

    public static void copy(InputStream inputStream, OutputStream outputStream) throws IOException {
        byte[] buf = new byte[1024];
        int read;
        while ((read = inputStream.read(buf)) >= 0) {
            outputStream.write(buf, 0, read);
        }
    }
}
