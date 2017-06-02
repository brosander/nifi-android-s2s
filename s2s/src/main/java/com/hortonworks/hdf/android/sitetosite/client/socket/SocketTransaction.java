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
import com.hortonworks.hdf.android.sitetosite.client.TransactionResult;
import com.hortonworks.hdf.android.sitetosite.client.protocol.CompressionOutputStream;
import com.hortonworks.hdf.android.sitetosite.client.protocol.RequestType;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.client.transaction.AbstractTransaction;
import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;

public class SocketTransaction extends AbstractTransaction {
    private final Socket socket;
    private final int protocolVersion;
    private int flowFilesSent = 0;

    public SocketTransaction(SocketPeerConnection socketPeerConnection, SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
        this.socket = socketPeerConnection.getSocket();
        this.protocolVersion = socketPeerConnection.getFlowFileProtocolVersion();

        Integer flowFileCodecVersion = socketPeerConnection.getFlowFileCodecVersion();
        if (flowFileCodecVersion == null) {
            throw new IOException("Need to negotiate flow file codec version before starting transaction");
        } else if (flowFileCodecVersion != 1) {
            throw new IOException("Unsupported codec version " + flowFileCodecVersion);
        }

        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        RequestType.SEND_FLOWFILES.writeRequestType(dataOutputStream);
        dataOutputStream.flush();
        OutputStream outputStream = socket.getOutputStream();
        if (siteToSiteClientConfig.isUseCompression()) {
            outputStream = new CompressionOutputStream(outputStream);
        }
        dataPacketWriter = new SocketDataPacketWriter(outputStream);
    }

    @Override
    public void send(DataPacket dataPacket) throws IOException {
        super.send(dataPacket);
        flowFilesSent++;
    }

    @Override
    public void confirm() throws IOException {
        String crc = Long.toString(dataPacketWriter.close());
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        ResponseCode.FINISH_TRANSACTION.writeResponse(dataOutputStream);
        dataOutputStream.flush();
        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
        ResponseCode responseCode = ResponseCode.readCode(dataInputStream);
        String message = null;
        if (responseCode.containsMessage()) {
            message = dataInputStream.readUTF();
        }
        if (responseCode == ResponseCode.CONFIRM_TRANSACTION) {
            if (protocolVersion > 3 && !crc.equals(message)) {
                throw new IOException("CRC received from peer didn't match");
            }
        } else {
            throw new IOException("Unexpected response code from peer: " + responseCode.name() + " and message " + message);
        }
    }

    @Override
    protected TransactionResult endTransaction(ResponseCode responseCodeToSend) throws IOException {
        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
        responseCodeToSend.writeResponse(dataOutputStream, "");
        dataOutputStream.flush();
        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
        ResponseCode responseCode = ResponseCode.readCode(dataInputStream);
        String message = null;
        if (responseCode.containsMessage()) {
            message = dataInputStream.readUTF();
        }
        RequestType.SHUTDOWN.writeRequestType(dataOutputStream);
        dataOutputStream.flush();
        return new TransactionResult(flowFilesSent, responseCode, message);
    }
}
