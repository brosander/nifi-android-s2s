package org.apache.nifi.android.sitetosite.client.transaction;

import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class DataPacketWriter {
    private final DataOutputStream dataOutputStream;
    private final CRC32 crc;

    public DataPacketWriter(OutputStream outputStream) {
        crc = new CRC32();
        dataOutputStream = new DataOutputStream(new CheckedOutputStream(outputStream, crc));
    }

    /**
     * Sends the dataPacket to NiFi
     *
     * @param dataPacket the dataPacket
     * @throws IOException if there is an error sending it
     */
    public void write(DataPacket dataPacket) throws IOException {
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
        dataOutputStream.close();
        return crc.getValue();
    }
}
