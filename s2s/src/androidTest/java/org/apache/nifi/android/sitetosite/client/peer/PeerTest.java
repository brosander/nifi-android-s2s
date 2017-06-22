package org.apache.nifi.android.sitetosite.client.peer;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.apache.nifi.android.sitetosite.client.http.HttpSiteToSiteClient;
import org.apache.nifi.android.sitetosite.util.MockNiFiS2SServer;
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

import static org.apache.nifi.android.sitetosite.client.http.HttpSiteToSiteClient.RECEIVED_RESPONSE_CODE;
import static org.apache.nifi.android.sitetosite.client.http.HttpSiteToSiteClient.WHEN_OPENING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PeerTest {

    /* Note, constructor is:                                                                                  *
     * Peer(String hostname, int httpPort, int rawPort, boolean secure, int flowFileCount, long lastFailure); */

    @Test
    public void testPeerCompareEqual() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L);
        Peer peer2 = new Peer("hostname", 443, 6000, false, 0, 1000L);

        assertEquals(0, peer1.compareTo(peer2));
        assertEquals(0, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByFailure() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L); // should be first due to older failure time
        Peer peer2 = new Peer("hostname", 443, 6000, false, 1, 2000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByFlowFileCount() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L); // should be first due to lower flow file count
        Peer peer2 = new Peer("hostname", 443, 6000, false, 1, 1000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByHostname() throws Exception {
        Peer peer1 = new Peer("hostname01", 443, 6000, false, 0, 1000L); // should be first due to hostname
        Peer peer2 = new Peer("hostname02", 443, 6000, false, 0, 1000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByHttpPort() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L); // should be first due to http port
        Peer peer2 = new Peer("hostname", 444, 6000, false, 0, 1000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByRawPort() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L); // should be first due to raw port
        Peer peer2 = new Peer("hostname", 443, 6001, false, 0, 1000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }

    @Test
    public void testPeerCompareByIsSecure() throws Exception {
        Peer peer1 = new Peer("hostname", 443, 6000, false, 0, 1000L); // should be first due to comparing bool cast to 0/1
        Peer peer2 = new Peer("hostname", 443, 6000, true, 0, 1000L);

        assertEquals(-1, peer1.compareTo(peer2));
        assertEquals(1, peer2.compareTo(peer1));
    }



}
