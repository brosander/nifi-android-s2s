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

import android.app.PendingIntent;
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
import org.apache.nifi.android.sitetosite.collectors.DataCollectorTestImpl;
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

public class SiteToSiteRepeatingTest {
    private MockNiFiS2SServer mockNiFiS2SServer;
    private SiteToSiteClientConfig siteToSiteClientConfig;
    private QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig;
    private Context context;
    private String portIdentifier;
    private String transactionIdentifier;
    private Peer peer;
    private SiteToSiteDB siteToSiteDB;
    private ParcelableTransactionResultCallbackTestImpl parcelableTransactionResultCallback;
    private ParcelableQueuedOperationResultCallbackTestImpl parcelableQueuedOperationResultCallback;

    @Before
    public void setup() throws IOException {
        context = InstrumentationRegistry.getContext();
        siteToSiteDB = SQLiteDataPacketQueueTest.getCleanSiteToSiteDB(context);

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
    public void testSendNoPackets() throws PendingIntent.CanceledException, InterruptedException {
        SiteToSiteRepeating.createSendPendingIntent(context, new DataCollectorTestImpl(), siteToSiteClientConfig, parcelableTransactionResultCallback).getPendingIntent().send();
        List<ParcelableTransactionResultCallbackTestImpl.Invocation> invocations = parcelableTransactionResultCallback.getInvocations();

        assertEquals(1, invocations.size());
        ParcelableTransactionResultCallbackTestImpl.Invocation invocation = invocations.get(0);
        assertNull(invocation.getIoException());
    }

    @Test(timeout = 5000)
    public void testSendSinglePacket() throws Exception {
        List<DataPacket> dataPackets = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8)));

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteRepeating.createSendPendingIntent(context, new DataCollectorTestImpl(dataPackets), siteToSiteClientConfig, parcelableTransactionResultCallback).getPendingIntent().send();
        List<ParcelableTransactionResultCallbackTestImpl.Invocation> invocations = parcelableTransactionResultCallback.getInvocations();

        assertEquals(1, invocations.size());
        ParcelableTransactionResultCallbackTestImpl.Invocation invocation = invocations.get(0);
        assertNull(invocation.getIoException());
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test/*(timeout = 5000)*/
    public void testSendMultipleBatches() throws Exception {
        List<DataPacket> batch1 = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8)));
        List<DataPacket> batch2 = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId2"), "testPayload2".getBytes(Charsets.UTF_8)));
        List<DataPacket> batch3 = Collections.emptyList();

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, batch1, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        PendingIntent pendingIntent = SiteToSiteRepeating.createSendPendingIntent(context, new DataCollectorTestImpl(batch1, batch2, batch3), siteToSiteClientConfig, parcelableTransactionResultCallback).getPendingIntent();
        pendingIntent.send();
        parcelableTransactionResultCallback.await();
        mockNiFiS2SServer.verifyAssertions();
        parcelableTransactionResultCallback.reinitCountDown(1);

        transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, batch2, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);
        pendingIntent.send();
        parcelableTransactionResultCallback.await();
        mockNiFiS2SServer.verifyAssertions();
        parcelableTransactionResultCallback.reinitCountDown(1);

        pendingIntent.send();

        List<ParcelableTransactionResultCallbackTestImpl.Invocation> invocations = parcelableTransactionResultCallback.getInvocations();

        assertEquals(3, invocations.size());
        for (ParcelableTransactionResultCallbackTestImpl.Invocation invocation : invocations) {
            if (invocation.getIoException() != null) {
                throw invocation.getIoException();
            }
        }

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testEnqueueNoPackets() throws PendingIntent.CanceledException, InterruptedException {
        SiteToSiteRepeating.createEnqueuePendingIntent(context, new DataCollectorTestImpl(), queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        List<ParcelableQueuedOperationResultCallbackTestImpl.Invocation> invocations = parcelableQueuedOperationResultCallback.getInvocations();

        assertEquals(1, invocations.size());
        ParcelableQueuedOperationResultCallbackTestImpl.Invocation invocation = invocations.get(0);
        assertNull(invocation.getIoException());
        assertNoQueuedPackets();
    }

    @Test(timeout = 5000)
    public void testEnqueueOnePacket() throws Exception {
        List<DataPacket> dataPackets = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8)));
        SiteToSiteRepeating.createEnqueuePendingIntent(context, new DataCollectorTestImpl(dataPackets), queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(1, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteRepeating.createProcessQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        List<ParcelableQueuedOperationResultCallbackTestImpl.Invocation> invocations = parcelableQueuedOperationResultCallback.getInvocations();
        mockNiFiS2SServer.verifyAssertions();

        assertEquals(2, invocations.size());
        for (ParcelableQueuedOperationResultCallbackTestImpl.Invocation invocation : invocations) {
            if (invocation.getIoException() != null) {
                throw invocation.getIoException();
            }
        }
        assertNoQueuedPackets();
    }

    @Test(timeout = 5000)
    public void testEnqueueMultiplePackets() throws Exception {
        List<DataPacket> batch1 = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8)));
        List<DataPacket> batch2 = Collections.<DataPacket>singletonList(new ByteArrayDataPacket(Collections.singletonMap("id", "testId2"), "testPayload2".getBytes(Charsets.UTF_8)));
        List<DataPacket> batch3 = Collections.emptyList();
        PendingIntent pendingIntent = SiteToSiteRepeating.createEnqueuePendingIntent(context, new DataCollectorTestImpl(batch1, batch2, batch3), queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent();
        pendingIntent.send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(1, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        pendingIntent.send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(2, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        pendingIntent.send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(2, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        List<DataPacket> dataPackets = new ArrayList<>();
        dataPackets.addAll(batch3);
        dataPackets.addAll(batch2);
        dataPackets.addAll(batch1);

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteRepeating.createProcessQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        List<ParcelableQueuedOperationResultCallbackTestImpl.Invocation> invocations = parcelableQueuedOperationResultCallback.getInvocations();
        mockNiFiS2SServer.verifyAssertions();

        assertEquals(4, invocations.size());
        for (ParcelableQueuedOperationResultCallbackTestImpl.Invocation invocation : invocations) {
            if (invocation.getIoException() != null) {
                throw invocation.getIoException();
            }
        }
        assertNoQueuedPackets();
    }

    @Test(timeout = 5000)
    public void testCleanup() throws Exception {
        queuedSiteToSiteClientConfig.setMaxRows(250);
        List<DataPacket> dataPackets = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        SiteToSiteRepeating.createEnqueuePendingIntent(context, new DataCollectorTestImpl(dataPackets), queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(500, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        SiteToSiteRepeating.createCleanupQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        parcelableQueuedOperationResultCallback.await();
        assertEquals(250, getQueuedPacketCount());
        parcelableQueuedOperationResultCallback.reinitCountDown(1);

        Collections.reverse(dataPackets);
        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets.subList(0, 100), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets.subList(100, 200), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets.subList(200, 250), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        SiteToSiteRepeating.createProcessQueuePendingIntent(context, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback).getPendingIntent().send();
        List<ParcelableQueuedOperationResultCallbackTestImpl.Invocation> invocations = parcelableQueuedOperationResultCallback.getInvocations();
        mockNiFiS2SServer.verifyAssertions();

        assertEquals(3, invocations.size());
        for (ParcelableQueuedOperationResultCallbackTestImpl.Invocation invocation : invocations) {
            if (invocation.getIoException() != null) {
                throw invocation.getIoException();
            }
        }
        assertNoQueuedPackets();
    }

    private void resetCallbacks() {
        resetQueuedCallback();
        resetTransactionCallback();
    }

    private void resetQueuedCallback() {
        parcelableQueuedOperationResultCallback = new ParcelableQueuedOperationResultCallbackTestImpl(1);
    }

    private void resetTransactionCallback() {
        parcelableTransactionResultCallback = new ParcelableTransactionResultCallbackTestImpl(1);
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
