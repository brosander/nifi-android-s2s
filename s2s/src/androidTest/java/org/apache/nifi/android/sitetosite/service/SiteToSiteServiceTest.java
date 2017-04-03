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
import android.support.test.InstrumentationRegistry;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDBTestUtil;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
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

import static org.junit.Assert.assertNull;

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
        siteToSiteDB = SiteToSiteDBTestUtil.getCleanSiteToSiteDB(context);

        mockNiFiS2SServer = new MockNiFiS2SServer();
        portIdentifier = "testPortIdentifier";
        transactionIdentifier = "testTransactionId";
        peer = new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0);

        siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setPortIdentifier(portIdentifier);
        SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
        siteToSiteRemoteCluster.setUrls(Collections.singleton(mockNiFiS2SServer.getNifiApiUrl()));
        siteToSiteClientConfig.setRemoteClusters(Collections.singletonList(siteToSiteRemoteCluster));

        queuedSiteToSiteClientConfig = new QueuedSiteToSiteClientConfig(siteToSiteClientConfig);

        queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
        transactionResultCallback = new TransactionResultCallbackTestImpl();
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

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testSendNoPackets() throws Exception {
        SiteToSiteService.sendDataPackets(context, Collections.<DataPacket>emptyList(), siteToSiteClientConfig, transactionResultCallback);
        assertNull(transactionResultCallback.getIOException());

        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 5000)
    public void testNoQueuedPackets() throws Exception {
        SiteToSiteService.enqueueDataPackets(context, Collections.<DataPacket>emptyList(), queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);

        queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());

        mockNiFiS2SServer.verifyAssertions();
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
    }

    @Test(timeout = 5000)
    public void testEnqueuePacket() throws Exception {
        DataPacket dataPacket = new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testData".getBytes(Charsets.UTF_8));

        SiteToSiteService.enqueueDataPacket(context, dataPacket, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, Collections.singletonList(dataPacket), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());

        mockNiFiS2SServer.verifyAssertions();
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
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
            queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
        }

        SiteToSiteDBTestUtil.assertQueuedPacketCount(siteToSiteDB, 500);

        SiteToSiteService.cleanupQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());

        SiteToSiteDBTestUtil.assertQueuedPacketCount(siteToSiteDB, 10);

        Collections.reverse(dataPackets);
        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 1, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        queuedOperationResultCallback = new QueuedOperationResultCallbackTestImpl();
        SiteToSiteService.processQueuedPackets(context, queuedSiteToSiteClientConfig, queuedOperationResultCallback);
        assertNull(queuedOperationResultCallback.getIOException());

        mockNiFiS2SServer.verifyAssertions();
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
    }
}
