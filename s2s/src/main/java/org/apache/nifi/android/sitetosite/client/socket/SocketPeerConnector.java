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

package org.apache.nifi.android.sitetosite.client.socket;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.protocol.RequestType;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

public class SocketPeerConnector {
    public static final byte[] MAGIC_BYTES = {(byte) 'N', (byte) 'i', (byte) 'F', (byte) 'i'};

    public static final int RESOURCE_OK = 20;
    public static final int DIFFERENT_RESOURCE_VERSION = 21;
    public static final int ABORT = 255;
    public static final String SOCKET_FLOW_FILE_PROTOCOL = "SocketFlowFileProtocol";
    public static final String GZIP = "GZIP";
    public static final String PORT_IDENTIFIER = "PORT_IDENTIFIER";
    public static final String REQUEST_EXPIRATION_MILLIS = "REQUEST_EXPIRATION_MILLIS";
    public static final String BATCH_COUNT = "BATCH_COUNT";
    public static final String BATCH_SIZE = "BATCH_SIZE";
    public static final String BATCH_DURATION = "BATCH_DURATION";
    public static final String STANDARD_FLOW_FILE_CODEC = "StandardFlowFileCodec";

    private final Peer peer;
    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final SiteToSiteRemoteCluster siteToSiteRemoteCluster;

    public SocketPeerConnector(Peer peer, SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster) {
        this.peer = peer;
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        this.siteToSiteRemoteCluster = siteToSiteRemoteCluster;
    }

    public SocketPeerConnection openConnection(boolean negotiateCodec) throws IOException {
        Socket socket;
        if (peer.isSecure()) {
            SSLContext sslContext = siteToSiteRemoteCluster.getSslContext();
            if (sslContext == null) {
                throw new IOException("SSL not configured but peer is set to secure");
            }
            socket = sslContext.getSocketFactory().createSocket(peer.getHostname(), peer.getRawPort());
        } else {
            socket = new Socket(peer.getHostname(), peer.getRawPort());
        }
        try {
            socket.setSoTimeout((int) siteToSiteClientConfig.getTimeout(TimeUnit.MILLISECONDS));
            socket.getOutputStream().write(MAGIC_BYTES);
            int protocolVersion = negotiateVersion(socket, SOCKET_FLOW_FILE_PROTOCOL, new int[] {6, 5, 4, 3, 2, 1});

            if (protocolVersion < 5 && siteToSiteClientConfig.getPortIdentifier() == null) {
                throw new IOException("Unable to find port identifier and it is required for this protocol version (" + protocolVersion + ")");
            }

            protocolHandshake(socket, protocolVersion);
            Integer codecVersion = null;
            if (negotiateCodec) {
                RequestType.NEGOTIATE_FLOWFILE_CODEC.writeRequestType(new DataOutputStream(socket.getOutputStream()));
                codecVersion = negotiateVersion(socket, STANDARD_FLOW_FILE_CODEC, new int[] {1});
            }
            return new SocketPeerConnection(socket, protocolVersion, codecVersion);
        } catch (IOException e) {
            socket.close();
            throw e;
        }
    }

    private int negotiateVersion(Socket socket, String resourceName, int[] versions) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        int maxVersion = Integer.MAX_VALUE;
        for (int version : versions) {
            if (version <= maxVersion) {
                dataOutputStream.writeUTF(resourceName);
                dataOutputStream.writeInt(version);
                dataOutputStream.flush();

                DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                int read = dataInputStream.read();
                if (read == RESOURCE_OK) {
                    return version;
                } else if (read == DIFFERENT_RESOURCE_VERSION) {
                    maxVersion = Math.min(maxVersion, dataInputStream.readInt());
                } else if (read == ABORT) {
                    throw new IOException("Abort received during negotiation: " + dataInputStream.readUTF());
                } else {
                    throw new IOException("Unknown response during protocol negotiation");
                }
            }
        }
        throw new IOException("Unable to agree on versions (server sent max version " + maxVersion + " we support " + Arrays.toString(versions) + ")");
    }

    private void protocolHandshake(Socket socket, int protocolVersion) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        dataOutputStream.writeUTF(UUID.randomUUID().toString());

        if (protocolVersion >= 3) {
            dataOutputStream.writeUTF(getPeerUri(peer));
        }

        final Map<String, String> properties = new HashMap<>();

        properties.put(GZIP, String.valueOf(siteToSiteClientConfig.isUseCompression()));

        String portIdentifier = siteToSiteClientConfig.getPortIdentifier();
        if (portIdentifier != null) {
            properties.put(PORT_IDENTIFIER, portIdentifier);
        }

        properties.put(REQUEST_EXPIRATION_MILLIS, String.valueOf(siteToSiteClientConfig.getTimeout(TimeUnit.MILLISECONDS)));

        if (protocolVersion >= 5) {
            int batchCount = siteToSiteClientConfig.getPreferredBatchCount();
            if (batchCount > 0) {
                properties.put(BATCH_COUNT, String.valueOf(batchCount));
            }

            long batchSize = siteToSiteClientConfig.getPreferredBatchSize();
            if (batchSize > 0L) {
                properties.put(BATCH_SIZE, String.valueOf(batchSize));
            }

            long batchMillis = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.MILLISECONDS);
            if (batchMillis > 0L) {
                properties.put(BATCH_DURATION, String.valueOf(batchMillis));
            }
        }

        dataOutputStream.writeInt(properties.size());
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            dataOutputStream.writeUTF(entry.getKey());
            dataOutputStream.writeUTF(entry.getValue());
        }
        dataOutputStream.flush();

        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
        ResponseCode responseCode = ResponseCode.readCode(dataInputStream);

        String message = null;
        if (responseCode.containsMessage()) {
            message = dataInputStream.readUTF();
        }

        if (responseCode != ResponseCode.PROPERTIES_OK) {
            throw new IOException("Error during handshake: " + responseCode + " with message " + message);
        }
    }

    protected static String getPeerUri(Peer peer) {
        return "nifi://" + peer.getHostname() + ":" + peer.getRawPort();
    }
}
