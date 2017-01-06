/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.android.sitetosite.packet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class FileDataPacketTest {
    public static final byte[] SOME_TEST_DATA = "some test data".getBytes(Charset.defaultCharset());
    private File tempFile;

    private FileDataPacket fileDataPacket;

    @Before
    public void setup() throws IOException {
        tempFile = File.createTempFile("abc", "def");
        tempFile.deleteOnExit();
        OutputStream outputStream = null;
        try {
            outputStream = new FileOutputStream(tempFile);
            outputStream.write(SOME_TEST_DATA);
        } finally {
            if (outputStream != null) {
                outputStream.close();
            }
        }
        fileDataPacket = new FileDataPacket(tempFile);
    }

    @After
    public void tearDown() {
        tempFile.delete();
    }

    @Test
    public void testAttributes() {
        Map<String, String> attributes = fileDataPacket.getAttributes();
        assertEquals(tempFile.getName(), attributes.get("filename"));
        assertEquals(tempFile.getParentFile().getPath(), attributes.get("path"));
        assertEquals(tempFile.getParentFile().getAbsolutePath(), attributes.get("absolute.path"));
    }

    @Test
    public void testGetData() throws IOException {
        byte[] bytes = new byte[(int) tempFile.length()];
        int index = 0;
        int read = 0;
        InputStream data = fileDataPacket.getData();
        try {
            while ((read = data.read(bytes, index, bytes.length - index)) != -1 && index < bytes.length) {
                index += read;
            }
        } finally {
            data.close();
        }
        assertEquals(new String(SOME_TEST_DATA, Charset.defaultCharset()), new String(bytes, Charset.defaultCharset()));
    }

    @Test
    public void testGetSize() {
        assertEquals(tempFile.length(), fileDataPacket.getSize());
    }
}
