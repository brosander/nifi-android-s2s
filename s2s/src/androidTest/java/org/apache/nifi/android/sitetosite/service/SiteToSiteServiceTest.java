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

package org.apache.nifi.android.sitetosite.service;

import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.support.test.InstrumentationRegistry;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.client.queued.db.SQLiteDataPacketQueueTest;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.apache.nifi.android.sitetosite.util.MockNiFiS2SServer;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class SiteToSiteServiceTest {
    private MockNiFiS2SServer mockNiFiS2SServer;
    private SiteToSiteClientConfig siteToSiteClientConfig;
    private QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig;
    private Context context;
    private String portIdentifier;
    private String transactionIdentifier;
    private Peer peer;
    private SiteToSiteDB siteToSiteDB;
    private QueuedOperationResultCallbackTestImpl queuedOperationResultCallback;
    private TransactionResultCallbackTestImpl transactionResultCallback;

    @Before
    public void setup() throws IOException {
        context = InstrumentationRegistry.getContext();
        siteToSiteDB = SQLiteDataPacketQueueTest.getSiteToSiteDBWithCleanDataPacketQueue(context);

        mockNiFiS2SServer = new MockNiFiS2SServer();
        portIdentifier = "testPortIdentifier";
        transactionIdentifier = "testTransactionId";
        peer = new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0);

        siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setPortIdentifier(portIdentifier);
        siteToSiteClientConfig.setUrls(Collections.singleton(mockNiFiS2SServer.getNifiApiUrl()));

        queuedSiteToSiteClientConfig = new QueuedSiteToSiteClientConfig(siteToSiteClientConfig);
        resetCallbacks();
    }

    @Test(timeout = 5000)
    public void testSendPacket() throws Exception {
        DataPacket dataPacket = new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testData".getBytes(Charsets.UTF_8));

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, Collections.singletonList(dataPacket), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteService.sendDataPacket(context, dataPacket, siteToSiteClientConfig, transactionResultCallback);
        assertNull(transactionResultCallback.getIOException());
        assertEquals(siteToSiteClientConfig.getUrls(), transactionResultCallback.getSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testSendPackets() throws Exception {
        List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testData".getBytes(Charsets.UTF_8)));
        }

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteService.sendDataPackets(context, dataPackets, siteToSiteClientConfig, transactionResultCallback);
        assertNull(transactionResultCallback.getIOException());
        assertEquals(siteToSiteClientConfig.getUrls(), transactionResultCallback.getSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testSendNoPackets() throws Exception {
        SiteToSiteService.sendDataPackets(context, Collections.<DataPacket>emptyList(), siteToSiteClientConfig, transactionResultCallback);
        assertNull(transactionResultCallback.getIOException());
        assertEquals(siteToSiteClientConfig.getUrls(), transactionResultCallback.getSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testNoQueuedPackets() throws Exception {
        SiteToSiteService.enqueueDataPackets(context, Collections.<DataPacket>emptyList(), queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();
        assertNoQueuedPackets();

        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());

        mockNiFiS2SServer.verifyAssertions();
        assertNoQueuedPackets();
    }

    @Test(timeout = 5000)
    public void testEnqueuePacket() throws Exception {
        DataPacket dataPacket = new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testData".getBytes(Charsets.UTF_8));

        SiteToSiteService.enqueueDataPacket(context, dataPacket, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, Collections.singletonList(dataPacket), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());

        mockNiFiS2SServer.verifyAssertions();
        assertNoQueuedPackets();
    }

    @Test(timeout = 5000)
    public void testCleanup() throws Exception {
        queuedSiteToSiteClientConfig.setMaxRows(10);

        List<DataPacket> dataPackets = new ArrayList<>(10);
        for (int i = 0; i < 500; i+= 10) {
            dataPackets.clear();
            for (int i1 = 0; i1 < 10; i1++) {
                dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + (i + i1)), ("testData" + (i + i1)).getBytes(Charsets.UTF_8)));
            }
            SiteToSiteService.enqueueDataPackets(context, dataPackets, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
            assertNull(queuedOperationResultCallback.getIOException());
            assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());
            resetQueuedCallback();
        }

        assertEquals(500, getQueuedPacketCount());

        SiteToSiteService.cleanupQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());
        resetQueuedCallback();

        assertEquals(10, getQueuedPacketCount());

        Collections.reverse(dataPackets);
        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        assertEquals(queuedSiteToSiteClientConfig.getUrls(), queuedOperationResultCallback.getQueuedSiteToSiteClientConfig().getUrls());

        mockNiFiS2SServer.verifyAssertions();
        assertNoQueuedPackets();
    }

    private void resetCallbacks() {
        resetQueuedCallback();
        resetTransactionCallback();
    }

    private void resetQueuedCallback() {
        queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
    }

    private void resetTransactionCallback() {
        transactionResultCallback = new TransactionResultCallbackTestImpl();
    }

    private void assertNoQueuedPackets() {
        assertEquals(0, getQueuedPacketCount());
    }

    private long getQueuedPacketCount() {
        SQLiteDatabase readableDatabase = siteToSiteDB.getReadableDatabase();
        try {
            Cursor query = readableDatabase.query(SiteToSiteDB.DATA_PACKET_QUEUE_TABLE_NAME, new String[]{"count(*) as numRows"}, null, null, null, null, null);
            try {
                assertTrue(query.moveToNext());
                return query.getLong(query.getColumnIndex("numRows"));
            } finally {
                query.close();
            }
        } finally {
            readableDatabase.close();
        }
    }
}
