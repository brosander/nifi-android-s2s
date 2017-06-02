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
import com.hortonworks.hdf.android.sitetosite.client.protocol.RequestType;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.packet.ByteArrayDataPacket;
import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;
import com.hortonworks.hdf.android.sitetosite.packet.EmptyDataPacket;
import com.hortonworks.hdf.android.sitetosite.util.Charsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class SocketTransactionTest {
    private ExecutorService executorService;
    private ServerSocket serverSocket;

    @Before
    public void setup() throws IOException {
        executorService = Executors.newSingleThreadExecutor();
        serverSocket = new ServerSocket(0);
    }

    @After
    public void teardown() {
        executorService.shutdown();
    }

    @Test
    public void testSendPackets() throws Exception {
        final List<DataPacket> dataPackets = new ArrayList<>();
        Map<String, String> attributes = new HashMap<>();
        attributes.put("key", "value");
        dataPackets.add(new EmptyDataPacket(attributes));
        attributes = new HashMap<>();
        attributes.put("key2", "value2");
        dataPackets.add(new ByteArrayDataPacket(attributes, "test data".getBytes(Charsets.UTF_8)));
        Future<Exception> future = executorService.submit(new Callable<Exception>() {
            @Override
            public Exception call() throws Exception {
                Socket socket = serverSocket.accept();
                try {
                    InputStream inputStream = socket.getInputStream();
                    DataInputStream dataInputStream = new DataInputStream(inputStream);
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    assertEquals(RequestType.SEND_FLOWFILES, RequestType.readRequestType(dataInputStream));
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    SocketDataPacketWriter socketDataPacketWriter = new SocketDataPacketWriter(outputStream);
                    for (DataPacket dataPacket : dataPackets) {
                        socketDataPacketWriter.write(dataPacket);
                    }
                    for (byte b : outputStream.toByteArray()) {
                        assertEquals(b, inputStream.read());
                    }
                    assertEquals(ResponseCode.FINISH_TRANSACTION, ResponseCode.readCode(inputStream));
                    ResponseCode.CONFIRM_TRANSACTION.writeResponse(dataOutputStream, Long.toString(socketDataPacketWriter.close()));
                    dataOutputStream.flush();
                    assertEquals(ResponseCode.CONFIRM_TRANSACTION, ResponseCode.readCode(inputStream));
                    assertEquals("", dataInputStream.readUTF());
                    ResponseCode.TRANSACTION_FINISHED.writeResponse(dataOutputStream);
                    dataOutputStream.flush();
                    assertEquals(RequestType.SHUTDOWN, RequestType.readRequestType(dataInputStream));
                } catch (Exception e) {
                    return e;
                }
                return null;
            }
        });
        SocketTransaction socketTransaction = new SocketTransaction(new SocketPeerConnection(new Socket("localhost", serverSocket.getLocalPort()), 6, 1), new SiteToSiteClientConfig());
        for (DataPacket dataPacket : dataPackets) {
            socketTransaction.send(dataPacket);
        }
        socketTransaction.confirm();
        socketTransaction.complete();
        Exception exception = future.get();
        if (exception != null) {
            throw exception;
        }
    }
}
