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

package org.apache.nifi.android.sitetosite.client.http.parser;

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PeerListParserTest {

    @Test
    public void testEmptyDocument() throws IOException {
        assertEquals(0, parsePeers(new JSONObject()).size());
    }

    @Test
    public void testEmptyPeersArray() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(PeerListParser.PEERS, new JSONArray());
        assertEquals(0, parsePeers(jsonObject).size());
    }

    @Test
    public void testSinglePeer() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();

        Peer peer = new Peer("testHostname", 9999, 0, true, 1234);

        value.put(toJsonObject(peer));
        jsonObject.put(PeerListParser.PEERS, value);

        List<Peer> peers = parsePeers(jsonObject);
        assertEquals(1, peers.size());
        assertEquals(peer, peers.get(0));
    }

    @Test
    public void testMultiplePeers() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();

        Peer peer = new Peer("testHostname", 9999, 0, true, 1234);
        Peer peer2 = new Peer("testHostname2", 9998, 0, false, 12345);

        value.put(toJsonObject(peer));
        value.put(toJsonObject(peer2));
        jsonObject.put(PeerListParser.PEERS, value);

        List<Peer> peers = parsePeers(jsonObject);
        assertEquals(2, peers.size());
        assertEquals(peer, peers.get(0));
        assertEquals(peer2, peers.get(1));
    }

    @Test
    public void testMissingHostname() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();
        value.put(toJsonObject(null, 1234, true, 111));
        jsonObject.put(PeerListParser.PEERS, value);
        assertEquals(0, parsePeers(jsonObject).size());
    }

    @Test
    public void testMissingPort() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();
        value.put(toJsonObject("testHostname", null, true, 111));
        jsonObject.put(PeerListParser.PEERS, value);
        assertEquals(0, parsePeers(jsonObject).size());
    }

    @Test
    public void testMissingSecure() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();
        value.put(toJsonObject("testHostname", 1234, null, 111));
        jsonObject.put(PeerListParser.PEERS, value);
        assertEquals(0, parsePeers(jsonObject).size());
    }

    @Test
    public void testMissingFlowFileCount() throws IOException, JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray value = new JSONArray();
        value.put(toJsonObject("testHostname", 1234, false, null));
        jsonObject.put(PeerListParser.PEERS, value);
        assertEquals(0, parsePeers(jsonObject).size());
    }

    private List<Peer> parsePeers(JSONObject jsonObject) throws IOException {
        return PeerListParser.parsePeers(new ByteArrayInputStream(jsonObject.toString().getBytes(Charsets.UTF_8)));
    }

    private JSONObject toJsonObject(Peer peer) throws JSONException {
        return toJsonObject(peer.getHostname(), peer.getHttpPort(), peer.isSecure(), peer.getFlowFileCount());
    }

    private JSONObject toJsonObject(String hostname, Integer port, Boolean isSecure, Integer flowFileCount) throws JSONException {
        JSONObject result = new JSONObject();
        if (hostname != null) {
            result.put(PeerListParser.HOSTNAME, hostname);
        }
        if (port != null) {
            result.put(PeerListParser.PORT, port);
        }
        if (isSecure != null) {
            result.put(PeerListParser.SECURE, isSecure);
        }
        if (flowFileCount != null) {
            result.put(PeerListParser.FLOW_FILE_COUNT, flowFileCount);
        }
        return result;
    }
}
