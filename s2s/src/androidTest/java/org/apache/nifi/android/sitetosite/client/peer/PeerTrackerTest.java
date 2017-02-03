package org.apache.nifi.android.sitetosite.client.peer;

import android.os.SystemClock;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.util.MockNiFiS2SServer;
import org.hamcrest.core.StringStartsWith;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;

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

    @Before
    public void setup() throws IOException {
        mockNiFiS2SServer = new MockNiFiS2SServer();

        flowFileCount = 100;
        nifiApiUrl = mockNiFiS2SServer.getNifiApiUrl();
        peer = new Peer(nifiApiUrl, flowFileCount);

        siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setUrls(Arrays.asList(nifiApiUrl));
        assertNull(siteToSiteClientConfig.getPeerStatus());
    }

    @Test
    public void testSuccessfulGetPeerStatus() throws Exception {
        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));

        new PeerTracker(siteToSiteClientConfig);

        PeerStatus peerStatus = siteToSiteClientConfig.getPeerStatus();
        List<Peer> peers = peerStatus.getPeers();
        assertEquals(1, peers.size());
        assertEquals(peer, peers.get(0));
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnsuccessfulGetPeerStatus() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(PeerTracker.RECEIVED_RESPONSE_CODE + 400 + PeerTracker.WHEN_OPENING + peer.getUrl());

        mockNiFiS2SServer.getMockWebServer().enqueue(new MockResponse().setResponseCode(400));
        new PeerTracker(siteToSiteClientConfig);
    }

    @Test
    public void testRefreshPeersAfterRestart() throws Exception {
        long before = SystemClock.elapsedRealtime();
        siteToSiteClientConfig.setPeerStatus(new PeerStatus(Arrays.asList(peer), Long.MAX_VALUE));

        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));

        new PeerTracker(siteToSiteClientConfig);

        long after = SystemClock.elapsedRealtime();

        mockNiFiS2SServer.verifyAssertions();
        PeerStatus peerStatus = siteToSiteClientConfig.getPeerStatus();
        assertTrue(peerStatus.getLastPeerUpdate() >= before);
        assertTrue(peerStatus.getLastPeerUpdate() <= after);
    }

    @Test
    public void testNoRefreshPeersIfJustDid() throws Exception {
        long lastPeerUpdate = SystemClock.elapsedRealtime() - 1;
        siteToSiteClientConfig.setPeerStatus(new PeerStatus(Arrays.asList(peer), lastPeerUpdate));

        new PeerTracker(siteToSiteClientConfig);

        assertEquals(lastPeerUpdate, siteToSiteClientConfig.getPeerStatus().getLastPeerUpdate());
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testGetPortIdentifier() throws Exception {
        String portIdentifier = "portId";
        String portName = "portName";

        mockNiFiS2SServer.enqueueSiteToSitePeers(Arrays.asList(peer));
        Map<String, String> nameToIdMap = new HashMap<>();
        nameToIdMap.put(portName, portIdentifier);
        mockNiFiS2SServer.enqueueInputPorts(nameToIdMap);

        assertEquals(portIdentifier, new PeerTracker(siteToSiteClientConfig).getPortIdentifier(portName));
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnsuccessfulResponseCode() throws IOException {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(new StringStartsWith(PeerTracker.RECEIVED_RESPONSE_CODE));

        mockNiFiS2SServer.getMockWebServer().enqueue(new MockResponse().setResponseCode(400));

        new PeerTracker(siteToSiteClientConfig);
    }
}
