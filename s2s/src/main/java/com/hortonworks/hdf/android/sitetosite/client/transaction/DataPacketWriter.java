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

package com.hortonworks.hdf.android.sitetosite.client.transaction;

import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;
import com.hortonworks.hdf.android.sitetosite.util.Charsets;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class DataPacketWriter {
    protected final DataOutputStream dataOutputStream;
    private final CRC32 crc;
    private final boolean closeStream;
    private boolean closed;

    public DataPacketWriter(OutputStream outputStream) {
        this(outputStream, true);
    }

    public DataPacketWriter(OutputStream outputStream, boolean closeStream) {
        crc = new CRC32();
        dataOutputStream = new DataOutputStream(new CheckedOutputStream(outputStream, crc));
        this.closeStream = closeStream;
        closed = false;
    }

    /**
     * Sends the dataPacket to NiFi
     *
     * @param dataPacket the dataPacket
     * @throws IOException if there is an error sending it
     */
    public void write(DataPacket dataPacket) throws IOException {
        if (closed) {
            throw new IOException("Tried to write after closing");
        }
        final Map<String, String> attributes = dataPacket.getAttributes();
        dataOutputStream.writeInt(attributes.size());
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            writeString(entry.getKey());
            writeString(entry.getValue());
        }

        dataOutputStream.writeLong(dataPacket.getSize());

        final InputStream in = dataPacket.getData();
        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read(buf)) != -1) {
            dataOutputStream.write(buf, 0, read);
        }
    }

    private void writeString(final String val) throws IOException {
        final byte[] bytes = val.getBytes(Charsets.UTF_8);
        dataOutputStream.writeInt(bytes.length);
        dataOutputStream.write(bytes);
    }

    /**
     * Closes the writer and returns a checksum of what it wrote
     *
     * @return a crc32 of the written data
     * @throws IOException if there was an error
     */
    public long close() throws IOException {
        closed = true;
        if (closeStream) {
            dataOutputStream.close();
        } else {
            dataOutputStream.flush();
        }
        return crc.getValue();
    }
}
