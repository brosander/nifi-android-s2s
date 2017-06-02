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

package com.hortonworks.hdf.android.sitetosite.client.socket;

import com.hortonworks.hdf.android.sitetosite.client.SiteToSiteClientConfig;
import com.hortonworks.hdf.android.sitetosite.client.SiteToSiteRemoteCluster;
import com.hortonworks.hdf.android.sitetosite.client.peer.Peer;
import com.hortonworks.hdf.android.sitetosite.client.protocol.RequestType;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hortonworks.hdf.android.sitetosite.client.socket.SocketPeerConnector.BATCH_COUNT;
import static com.hortonworks.hdf.android.sitetosite.client.socket.SocketPeerConnector.BATCH_DURATION;
import static com.hortonworks.hdf.android.sitetosite.client.socket.SocketPeerConnector.BATCH_SIZE;
import static com.hortonworks.hdf.android.sitetosite.client.socket.SocketPeerConnector.PORT_IDENTIFIER;
import static com.hortonworks.hdf.android.sitetosite.client.socket.SocketPeerConnector.REQUEST_EXPIRATION_MILLIS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SocketPeerConnectorTest {
    private ExecutorService executorService;

    @Before
    public void setup() {
        executorService = Executors.newSingleThreadExecutor();
    }

    @After
    public void teardown() {
        executorService.shutdown();
    }

    @Test
    public void testSocketPeerConnector() throws Exception {
        final String portIdentifier = "abcd";
        final ServerSocket serverSocket = new ServerSocket(0);
        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setPortIdentifier(portIdentifier);
        final Peer peer = new Peer("localhost", 0, serverSocket.getLocalPort(), false, 0);

        final Future<Exception> future = startServer(serverSocket, peer, siteToSiteClientConfig, true);

        SocketPeerConnector connector = new SocketPeerConnector(peer, siteToSiteClientConfig, new SiteToSiteRemoteCluster());
        SocketPeerConnection socketPeerConnection = connector.openConnection(true);
        Exception exception = future.get();
        if (exception != null) {
            throw exception;
        }
        assertEquals(6, socketPeerConnection.getFlowFileProtocolVersion());
        assertEquals(1, (int) socketPeerConnection.getFlowFileCodecVersion());
    }

    @Test
    public void testSocketPeerConnectorNoCodec() throws Exception {
        final String portIdentifier = "abcd";
        final ServerSocket serverSocket = new ServerSocket(0);
        final SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setPortIdentifier(portIdentifier);
        final Peer peer = new Peer("localhost", 0, serverSocket.getLocalPort(), false, 0);

        final Future<Exception> future = startServer(serverSocket, peer, siteToSiteClientConfig, false);

        SocketPeerConnector connector = new SocketPeerConnector(peer, siteToSiteClientConfig, new SiteToSiteRemoteCluster());
        SocketPeerConnection socketPeerConnection = connector.openConnection(false);
        Exception exception = future.get();
        if (exception != null) {
            throw exception;
        }
        assertEquals(6, socketPeerConnection.getFlowFileProtocolVersion());
        assertNull(socketPeerConnection.getFlowFileCodecVersion());
    }

    private Future<Exception> startServer(final ServerSocket serverSocket, final Peer peer, final SiteToSiteClientConfig siteToSiteClientConfig, final boolean negotiateCodec) {
        return executorService.submit(new Callable<Exception>() {
                @Override
                public Exception call() throws Exception {
                    Socket socket = serverSocket.accept();
                    try {
                        InputStream inputStream = socket.getInputStream();
                        OutputStream outputStream = socket.getOutputStream();
                        assertMagicBytes(inputStream);
                        DataInputStream dataInputStream = new DataInputStream(inputStream);
                        DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
                        int serverNegotiate = serverNegotiate(dataOutputStream, dataInputStream, SocketPeerConnector.SOCKET_FLOW_FILE_PROTOCOL, new int[]{6}, new int[0]);
                        assertNotNull(UUID.fromString(dataInputStream.readUTF()));
                        if (serverNegotiate >= 3) {
                            assertEquals(SocketPeerConnector.getPeerUri(peer), dataInputStream.readUTF());
                        }

                        Map<String, String> expectedProperties = new HashMap<>();
                        expectedProperties.put(SocketPeerConnector.GZIP, Boolean.toString(siteToSiteClientConfig.isUseCompression()));
                        String portIdentifier = siteToSiteClientConfig.getPortIdentifier();
                        if (portIdentifier != null) {
                            expectedProperties.put(PORT_IDENTIFIER, portIdentifier);
                        }
                        expectedProperties.put(REQUEST_EXPIRATION_MILLIS, Long.toString(siteToSiteClientConfig.getTimeout(TimeUnit.MILLISECONDS)));
                        if (serverNegotiate >= 5) {
                            int batchCount = siteToSiteClientConfig.getPreferredBatchCount();
                            if (batchCount > 0) {
                                expectedProperties.put(BATCH_COUNT, String.valueOf(batchCount));
                            }

                            long batchSize = siteToSiteClientConfig.getPreferredBatchSize();
                            if (batchSize > 0L) {
                                expectedProperties.put(BATCH_SIZE, String.valueOf(batchSize));
                            }

                            long batchMillis = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.MILLISECONDS);
                            if (batchMillis > 0L) {
                                expectedProperties.put(BATCH_DURATION, String.valueOf(batchMillis));
                            }
                        }
                        assertProperties(dataInputStream, expectedProperties);
                        ResponseCode.PROPERTIES_OK.writeResponse(dataOutputStream);
                        dataOutputStream.flush();

                        if (negotiateCodec) {
                            assertEquals(RequestType.NEGOTIATE_FLOWFILE_CODEC, RequestType.readRequestType(dataInputStream));
                            serverNegotiate(dataOutputStream, dataInputStream, SocketPeerConnector.STANDARD_FLOW_FILE_CODEC, new int[]{1}, new int[0]);
                        }

                        socket.close();
                        return null;
                    } catch (Exception e) {
                        return e;
                    } finally {
                        socket.close();
                    }
                }
            });
    }

    private void assertMagicBytes(InputStream inputStream) throws IOException {
        for (int i = 0; i < SocketPeerConnector.MAGIC_BYTES.length; i++) {
            assertEquals("Expected magic bytes at start", SocketPeerConnector.MAGIC_BYTES[i], inputStream.read());
        }
    }

    private int serverNegotiate(DataOutputStream dataOutputStream, DataInputStream dataInputStream, String resourceName, int[] expectedSuggestions, int[] responses) throws IOException {
        for (int i = 0; i < expectedSuggestions.length; i++) {
            assertEquals(resourceName, dataInputStream.readUTF());
            assertEquals(expectedSuggestions[i], dataInputStream.readInt());
            if (responses.length <= i) {
                dataOutputStream.write(SocketPeerConnector.RESOURCE_OK);
                return expectedSuggestions[i];
            } else {
                dataOutputStream.write(SocketPeerConnector.DIFFERENT_RESOURCE_VERSION);
                dataOutputStream.writeInt(responses[i]);
            }
            dataOutputStream.flush();
        }
        throw new IOException("Negotiation failed");
    }

    private void assertProperties(DataInputStream dataInputStream, Map<String, String> expected) throws IOException {
        int mapSize = dataInputStream.readInt();
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < mapSize; i++) {
            properties.put(dataInputStream.readUTF(), dataInputStream.readUTF());
        }
        assertEquals(expected, properties);
    }
}
