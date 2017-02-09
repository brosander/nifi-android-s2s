package org.apache.nifi.android.sitetosite.client.protocol;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public enum RequestType {
    NEGOTIATE_FLOWFILE_CODEC,
    REQUEST_PEER_LIST,
    SEND_FLOWFILES,
    RECEIVE_FLOWFILES,
    SHUTDOWN;

    public void writeRequestType(final DataOutputStream dos) throws IOException {
        dos.writeUTF(name());
    }

    public static RequestType readRequestType(final DataInputStream dis) throws IOException {
        final String requestTypeVal = dis.readUTF();
        try {
            return RequestType.valueOf(requestTypeVal);
        } catch (final Exception e) {
            throw new IOException("Could not determine RequestType: received invalid value " + requestTypeVal);
        }
    }
}
