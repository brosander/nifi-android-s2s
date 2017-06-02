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

import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.client.transaction.DataPacketWriter;
import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class SocketDataPacketWriter extends DataPacketWriter {
    private final DataOutputStream dataOutputStream;
    boolean first = true;

    public SocketDataPacketWriter(OutputStream outputStream) {
        super(outputStream, false);
        this.dataOutputStream = new DataOutputStream(outputStream);
    }

    @Override
    public void write(DataPacket dataPacket) throws IOException {
        if (first) {
            first = false;
        } else {
            ResponseCode.CONTINUE_TRANSACTION.writeResponse(dataOutputStream);
        }
        super.write(dataPacket);
    }
}
