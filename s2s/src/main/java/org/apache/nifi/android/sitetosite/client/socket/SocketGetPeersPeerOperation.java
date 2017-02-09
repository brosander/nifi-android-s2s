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

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.peer.PeerOperation;
import org.apache.nifi.android.sitetosite.client.protocol.RequestType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class SocketGetPeersPeerOperation implements PeerOperation<List<Peer>, SocketPeerConnector> {

    @Override
    public List<Peer> perform(Peer peer, SocketPeerConnector connectionManager) throws IOException {
        Socket socket = connectionManager.openConnection(false).getSocket();
        OutputStream outputStream = socket.getOutputStream();
        InputStream inputStream = socket.getInputStream();
        DataOutputStream dos = new DataOutputStream(outputStream);
        try {
            RequestType.REQUEST_PEER_LIST.writeRequestType(dos);
            dos.flush();
            DataInputStream dis = new DataInputStream(inputStream);
            int numPeers = dis.readInt();
            List<Peer> result = new ArrayList<>();
            for (int i = 0; i < numPeers; i++) {
                final String hostname = dis.readUTF();
                final int port = dis.readInt();
                final boolean secure = dis.readBoolean();
                final int flowFileCount = dis.readInt();
                result.add(new Peer(hostname, 0, port, secure, flowFileCount));
            }
            return result;
        } finally {
            try {
                RequestType.SHUTDOWN.writeRequestType(dos);
                dos.flush();
            } catch (IOException e) {
                // Ignore
            }

            try {
                outputStream.close();
            } catch (IOException e) {
                // Ignore
            }
            try {
                inputStream.close();
            } catch (IOException e) {
                // Ignore
            }
            try {
                socket.close();
            } catch (IOException e) {
                // Ignore
            }
        }
    }
}
