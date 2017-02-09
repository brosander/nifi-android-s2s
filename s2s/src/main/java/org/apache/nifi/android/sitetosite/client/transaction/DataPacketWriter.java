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
