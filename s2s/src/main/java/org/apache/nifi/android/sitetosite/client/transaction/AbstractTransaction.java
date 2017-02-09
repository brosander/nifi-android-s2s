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

package org.apache.nifi.android.sitetosite.client.transaction;

import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.packet.DataPacket;

import java.io.IOException;
import java.io.OutputStream;

/**
 * AbstractTransaction for sending data to a NiFi instance
 */
public abstract class AbstractTransaction implements Transaction {
    protected DataPacketWriter dataPacketWriter;

    @Override
    public void send(DataPacket dataPacket) throws IOException {
        dataPacketWriter.write(dataPacket);
    }

    @Override
    public TransactionResult complete() throws IOException {
        return endTransaction(ResponseCode.CONFIRM_TRANSACTION);
    }

    @Override
    public TransactionResult cancel() throws IOException {
        return endTransaction(ResponseCode.CANCEL_TRANSACTION);
    }

    protected abstract TransactionResult endTransaction(ResponseCode responseCodeToSend) throws IOException;
}
