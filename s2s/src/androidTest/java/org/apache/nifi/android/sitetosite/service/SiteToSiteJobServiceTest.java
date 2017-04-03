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
import android.app.job.JobInfo;
import android.app.job.JobScheduler;
import android.content.Context;
import android.os.Build;
import android.support.annotation.RequiresApi;
import android.support.test.InstrumentationRegistry;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
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

import static org.junit.Assert.assertEquals;

@RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
public class SiteToSiteJobServiceTest {
    private MockNiFiS2SServer mockNiFiS2SServer;
    private QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig;
    private Context context;
    private String portIdentifier;
    private String transactionIdentifier;
    private Peer peer;
    private SiteToSiteDB siteToSiteDB;
    private ParcelableQueuedOperationResultCallbackTestImpl parcelableQueuedOperationResultCallback;

    @Before
    public void setup() throws IOException {
        context = InstrumentationRegistry.getContext();
        siteToSiteDB = SiteToSiteDBTestUtil.getCleanSiteToSiteDB(context);

        mockNiFiS2SServer = new MockNiFiS2SServer();
        portIdentifier = "testPortIdentifier";
        transactionIdentifier = "testTransactionId";
        peer = new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0);

        queuedSiteToSiteClientConfig = new QueuedSiteToSiteClientConfig();
        queuedSiteToSiteClientConfig.setPortIdentifier(portIdentifier);
        SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
        siteToSiteRemoteCluster.setUrls(Collections.singleton(mockNiFiS2SServer.getNifiApiUrl()));
        queuedSiteToSiteClientConfig.setRemoteClusters(Collections.singletonList(siteToSiteRemoteCluster));

        parcelableQueuedOperationResultCallback = new ParcelableQueuedOperationResultCallbackTestImpl();
    }

    @Test(timeout = 5000)
    public void testProcessNoPackets() throws PendingIntent.CanceledException, InterruptedException {
        JobInfo.Builder processJobInfoBuilder = SiteToSiteJobService.createProcessJobInfoBuilder(context, 1, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
        processJobInfoBuilder.setOverrideDeadline(0);
        JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
        assertEquals(JobScheduler.RESULT_SUCCESS, jobScheduler.schedule(processJobInfoBuilder.build()));
        assertEquals(1, parcelableQueuedOperationResultCallback.getInvocations().size());
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
    }

    @Test(timeout = 5000)
    public void testProcessOnePacket() throws Exception {
        DataPacket dataPacket = new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8));
        queuedSiteToSiteClientConfig.getQueuedSiteToSiteClient(context).enqueue(dataPacket);

        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, Collections.singletonList(dataPacket), queuedSiteToSiteClientConfig);
        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 2, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);

        JobInfo.Builder processJobInfoBuilder = SiteToSiteJobService.createProcessJobInfoBuilder(context, 1, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
        processJobInfoBuilder.setOverrideDeadline(0);
        JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
        assertEquals(JobScheduler.RESULT_SUCCESS, jobScheduler.schedule(processJobInfoBuilder.build()));
        assertEquals(1, parcelableQueuedOperationResultCallback.getInvocations().size());
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test(timeout = 25000)
    public void temstProcessAThousandPackets() throws Exception {
        int numPackets = 1000;
        List<DataPacket> dataPackets = new ArrayList<>(numPackets);
        for (int i = 0; i < numPackets; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        queuedSiteToSiteClientConfig.getQueuedSiteToSiteClient(context).enqueue(dataPackets.iterator());
        SiteToSiteDBTestUtil.assertQueuedPacketCount(siteToSiteDB, numPackets);

        Collections.reverse(dataPackets);
        mockNiFiS2SServer.enqueueSiteToSitePeers(Collections.singletonList(peer));

        for (int i = 0; i < numPackets; i+= 100) {
            String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);
            mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets.subList(i, i + 100), queuedSiteToSiteClientConfig);
            mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, 2, ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);
        }

        JobInfo.Builder processJobInfoBuilder = SiteToSiteJobService.createProcessJobInfoBuilder(context, 3, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
        processJobInfoBuilder.setOverrideDeadline(0);
        JobScheduler jobScheduler = (JobScheduler) context.getSystemService(Context.JOB_SCHEDULER_SERVICE);
        assertEquals(JobScheduler.RESULT_SUCCESS, jobScheduler.schedule(processJobInfoBuilder.build()));
        assertEquals(1, parcelableQueuedOperationResultCallback.getInvocations().size());
        SiteToSiteDBTestUtil.assertNoQueuedPackets(siteToSiteDB);
        mockNiFiS2SServer.verifyAssertions();
    }
}
