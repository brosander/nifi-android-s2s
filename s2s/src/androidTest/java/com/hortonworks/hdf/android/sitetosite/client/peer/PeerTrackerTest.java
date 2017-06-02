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

package com.hortonworks.hdf.android.sitetosite.client.peer;

import com.hortonworks.hdf.android.sitetosite.client.SiteToSiteClientConfig;
import com.hortonworks.hdf.android.sitetosite.client.SiteToSiteRemoteCluster;
import com.hortonworks.hdf.android.sitetosite.client.http.HttpSiteToSiteClient;
import com.hortonworks.hdf.android.sitetosite.util.MockNiFiS2SServer;
import org.hamcrest.Matchers;
import org.hamcrest.core.StringStartsWith;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;

import static com.hortonworks.hdf.android.sitetosite.client.http.HttpSiteToSiteClient.RECEIVED_RESPONSE_CODE;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpSiteToSiteClient.WHEN_OPENING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PeerTrackerTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private MockNiFiS2SServer mockNiFiS2SServer;
    private Peer peer;
    private int flowFileCount;
    private String nifiApiUrl;
    private SiteToSiteClientConfig siteToSiteClientConfig;
    private SiteToSiteRemoteCluster siteToSiteRemoteCluster;

    @Before
    public void setup() throws IOException {
        mockNiFiS2SServer = new MockNiFiS2SServer();

        flowFileCount = 100;
        nifiApiUrl = mockNiFiS2SServer.getNifiApiUrl();
        peer = new Peer(nifiApiUrl, flowFileCount);

        siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
        siteToSiteRemoteCluster.setUrls(Collections.singleton(nifiApiUrl));
        siteToSiteClientConfig.setRemoteClusters(Collections.singletonList(siteToSiteRemoteCluster));
        assertNull(siteToSiteRemoteCluster.getPeerStatus());
    }

    @Test
    public void testSuccessfulGetPeerStatus() throws Exception {
        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));
        String portName = "portName";
        mockNiFiS2SServer.enqueueInputPorts(Collections.singletonMap(portName, "portId"));
        siteToSiteClientConfig.setPortName(portName);

        new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);

        PeerStatus peerStatus = siteToSiteRemoteCluster.getPeerStatus();
        List<Peer> peers = peerStatus.getPeers();
        assertEquals(1, peers.size());
        assertEquals(peer, peers.get(0));
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnsuccessfulGetPeerStatus() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(Matchers.startsWith(RECEIVED_RESPONSE_CODE + 400 + WHEN_OPENING));

        mockNiFiS2SServer.getMockWebServer().enqueue(new MockResponse().setResponseCode(400));
        new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);
    }

    @Test
    public void testRefreshPeers() throws Exception {
        long before = System.currentTimeMillis();
        siteToSiteRemoteCluster.setPeerStatus(new PeerStatus(Arrays.asList(peer), 0L));

        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));
        String portName = "portName";
        mockNiFiS2SServer.enqueueInputPorts(Collections.singletonMap(portName, "portId"));
        siteToSiteClientConfig.setPortName(portName);

        new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);

        long after = System.currentTimeMillis();

        mockNiFiS2SServer.verifyAssertions();
        PeerStatus peerStatus = siteToSiteRemoteCluster.getPeerStatus();
        assertTrue(peerStatus.getLastPeerUpdate() >= before);
        assertTrue(peerStatus.getLastPeerUpdate() <= after);
    }

    @Test
    public void testNoRefreshPeersIfJustDid() throws Exception {
        long lastPeerUpdate = System.currentTimeMillis() - 1;
        siteToSiteRemoteCluster.setPeerStatus(new PeerStatus(Arrays.asList(peer), lastPeerUpdate));
        String portName = "portName";
        mockNiFiS2SServer.enqueueInputPorts(Collections.singletonMap(portName, "portId"));
        siteToSiteClientConfig.setPortName(portName);

        new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);

        assertEquals(lastPeerUpdate, siteToSiteRemoteCluster.getPeerStatus().getLastPeerUpdate());
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testGetPortIdentifier() throws Exception {
        String portIdentifier = "portId";
        String portName = "portName";

        siteToSiteClientConfig.setPortName(portName);
        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));
        Map<String, String> nameToIdMap = new HashMap<>();
        nameToIdMap.put(portName, portIdentifier);
        mockNiFiS2SServer.enqueueInputPorts(nameToIdMap);

        assertEquals(portIdentifier, new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster).getPortIdentifier());
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnsuccessfulResponseCode() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(new StringStartsWith(RECEIVED_RESPONSE_CODE));

        mockNiFiS2SServer.getMockWebServer().enqueue(new MockResponse().setResponseCode(400));

        new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);
    }
}
