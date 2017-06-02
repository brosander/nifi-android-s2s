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

package com.hortonworks.hdf.android.sitetosite.client;

import com.hortonworks.hdf.android.sitetosite.client.http.HttpPeerConnector;
import com.hortonworks.hdf.android.sitetosite.client.http.HttpTransaction;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.packet.ByteArrayDataPacket;
import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;
import com.hortonworks.hdf.android.sitetosite.util.Charsets;
import com.hortonworks.hdf.android.sitetosite.util.MockNiFiS2SServer;
import com.hortonworks.hdf.android.sitetosite.util.MockScheduledExecutor;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import okhttp3.mockwebserver.RecordedRequest;

import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_COUNT;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_DURATION;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.HANDSHAKE_PROPERTY_BATCH_SIZE;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.HANDSHAKE_PROPERTY_USE_COMPRESSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class HttpTransactionTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    String portIdentifier;
    String transactionIdentifier;
    private MockScheduledExecutor scheduledThreadPoolExecutor;
    private MockNiFiS2SServer mockNiFiS2SServer;

    @Before
    public void setup() throws IOException {
        scheduledThreadPoolExecutor = new MockScheduledExecutor(1);
        mockNiFiS2SServer = new MockNiFiS2SServer();

        portIdentifier = "testPortIdentifier";
        transactionIdentifier = "testTransactionId";
    }

    @Test
    public void testSuccessfulTransaction() throws Exception {
        performTestSuccessfulTransaction(new SiteToSiteClientConfig());
    }

    @Test
    public void testDefaultHandshakeProperties() throws Exception {
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();

        List<RecordedRequest> recordedRequests = performTestSuccessfulTransaction(siteToSiteClientConfig);

        for (RecordedRequest recordedRequest : recordedRequests) {
            assertNull(recordedRequest.getHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION));
            assertEquals(Long.toString(siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION));
            assertEquals(Integer.toString(siteToSiteClientConfig.getPreferredBatchCount()), recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_COUNT));
            assertNull(recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_SIZE));
            assertNull(recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_DURATION));
        }
    }

    @Test
    public void testNonDefaultHandshakeProperties() throws Exception {
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setIdleConnectionExpiration(10, TimeUnit.SECONDS);
        siteToSiteClientConfig.setUseCompression(true);
        siteToSiteClientConfig.setPreferredBatchCount(25);
        siteToSiteClientConfig.setPreferredBatchSize(100);
        siteToSiteClientConfig.setPreferredBatchDuration(60, TimeUnit.SECONDS);

        List<RecordedRequest> recordedRequests = performTestSuccessfulTransaction(siteToSiteClientConfig);

        for (RecordedRequest recordedRequest : recordedRequests) {
            assertEquals(Boolean.toString(true), recordedRequest.getHeader(HANDSHAKE_PROPERTY_USE_COMPRESSION));
            assertEquals(Long.toString(siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION));
            assertEquals(Integer.toString(siteToSiteClientConfig.getPreferredBatchCount()), recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_COUNT));
            assertEquals(Long.toString(siteToSiteClientConfig.getPreferredBatchSize()), recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_SIZE));
            assertEquals(Long.toString(siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(HANDSHAKE_PROPERTY_BATCH_DURATION));
        }
    }

    @Test
    public void testNoTransactionUrlIntent() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(HttpTransaction.EXPECTED_TRANSACTION_URL_AS_INTENT);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, null, 30);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new HttpTransaction(new HttpPeerConnector(mockNiFiS2SServer.getNifiApiUrl(), siteToSiteClientConfig, new SiteToSiteRemoteCluster()), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testExpectedTransactionUrl() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(HttpTransaction.EXPECTED_TRANSACTION_URL);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30, false);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new HttpTransaction(new HttpPeerConnector(mockNiFiS2SServer.getNifiApiUrl(), siteToSiteClientConfig, new SiteToSiteRemoteCluster()), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testExpectedTtl() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(HttpTransaction.EXPECTED_TTL);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, null);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new HttpTransaction(new HttpPeerConnector(mockNiFiS2SServer.getNifiApiUrl(), siteToSiteClientConfig, new SiteToSiteRemoteCluster()), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnparseableTtl() throws Exception {
        expectedException.expect(IOException.class);
        String ttl = "abcd";
        expectedException.expectMessage(HttpTransaction.UNABLE_TO_PARSE_TTL + ttl);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, ttl);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new HttpTransaction(new HttpPeerConnector(mockNiFiS2SServer.getNifiApiUrl(), siteToSiteClientConfig, new SiteToSiteRemoteCluster()), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    private List<RecordedRequest> performTestSuccessfulTransaction(SiteToSiteClientConfig siteToSiteClientConfig) throws Exception {
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);

        mockNiFiS2SServer.enqueueTtlExtension(transactionPath);

        List<DataPacket> dataPackets = Arrays.<DataPacket>asList(new ByteArrayDataPacket(new HashMap<String, String>(), "testMessage".getBytes(Charsets.UTF_8)));
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, siteToSiteClientConfig);

        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, dataPackets.size(), ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);
        HttpTransaction httpTransaction = new HttpTransaction(new HttpPeerConnector(mockNiFiS2SServer.getNifiApiUrl(), siteToSiteClientConfig, new SiteToSiteRemoteCluster()), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        scheduledThreadPoolExecutor.getTtlExtender(15).run();

        for (DataPacket dataPacket : dataPackets) {
            httpTransaction.send(dataPacket);
        }
        httpTransaction.confirm();
        httpTransaction.complete();
        return mockNiFiS2SServer.verifyAssertions();
    }
}
