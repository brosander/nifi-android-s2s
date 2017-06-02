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

package com.hortonworks.hdf.android.sitetosite.util;

import com.hortonworks.hdf.android.sitetosite.client.SiteToSiteClientConfig;
import com.hortonworks.hdf.android.sitetosite.client.http.HttpMethod;
import com.hortonworks.hdf.android.sitetosite.client.http.HttpTransaction;
import com.hortonworks.hdf.android.sitetosite.client.http.parser.PeerListParser;
import com.hortonworks.hdf.android.sitetosite.client.http.parser.TransactionResultParser;
import com.hortonworks.hdf.android.sitetosite.client.peer.Peer;
import com.hortonworks.hdf.android.sitetosite.client.peer.PeerTracker;
import com.hortonworks.hdf.android.sitetosite.client.peer.SiteToSiteInfo;
import com.hortonworks.hdf.android.sitetosite.client.protocol.CompressionOutputStream;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.client.transaction.DataPacketWriter;
import com.hortonworks.hdf.android.sitetosite.packet.DataPacket;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.ACCEPT;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.CONTENT_TYPE;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.LOCATION_HEADER_NAME;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpHeaders.SERVER_SIDE_TRANSACTION_TTL;
import static com.hortonworks.hdf.android.sitetosite.client.http.HttpSiteToSiteClient.SITE_TO_SITE_PEERS_PATH;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MockNiFiS2SServer {
    private final MockWebServer mockWebServer;
    private final List<RequestAssertion> requestAssertions;
    private int verifyIndex = 0;
    private final RequestAssertion siteToSitePeerListRequestAssertion;

    public MockNiFiS2SServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        requestAssertions = new ArrayList<>();

        siteToSitePeerListRequestAssertion = new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(PeerTracker.NIFI_API_PATH + SITE_TO_SITE_PEERS_PATH, recordedRequest.getPath());
                return recordedRequest;
            }
        };
    }

    public String getNifiApiUrl() {
        return mockWebServer.url("/nifi-api").toString();
    }

    public RequestAssertion getSiteToSitePeerListRequestAssertion() {
        return siteToSitePeerListRequestAssertion;
    }

    public List<RecordedRequest> verifyAssertions() throws Exception {
        int requestAssertionCount = requestAssertions.size();
        assertEquals(requestAssertionCount, mockWebServer.getRequestCount());
        List<RecordedRequest> result = new ArrayList<>(requestAssertionCount);
        for (RequestAssertion requestAssertion : requestAssertions.subList(verifyIndex, requestAssertionCount)) {
            result.add(requestAssertion.check());
        }
        verifyIndex = requestAssertionCount;
        return result;
    }

    public String enqueuCreateTransaction(String portIdentifier, String transactionIdentifier, Object ttl) {
        return enqueuCreateTransaction(portIdentifier, transactionIdentifier, ttl, true);
    }

    public String enqueuCreateTransaction(String portIdentifier, String transactionIdentifier, Object ttl, boolean addLocationHeader) {
        final String transactionsPath = "/nifi-api/data-transfer/input-ports/" + portIdentifier + "/transactions";
        String transactionPath = transactionsPath + "/" + transactionIdentifier;
        MockResponse mockResponse = new MockResponse();
        if (transactionIdentifier != null) {
            mockResponse = mockResponse.addHeader(LOCATION_URI_INTENT_NAME, LOCATION_URI_INTENT_VALUE);
        }
        if (addLocationHeader) {
            mockResponse = mockResponse.addHeader(LOCATION_HEADER_NAME, mockWebServer.url(transactionPath));
        }
        if (ttl != null) {
            mockResponse = mockResponse.addHeader(SERVER_SIDE_TRANSACTION_TTL, ttl.toString());
        }
        enqueue(mockResponse, new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(transactionsPath, recordedRequest.getPath());
                assertEquals(HttpMethod.POST.name(), recordedRequest.getMethod());
                return recordedRequest;
            }
        });
        return transactionPath;
    }

    public void enqueueTtlExtension(final String transactionPath) {
        enqueue(new MockResponse(), new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(transactionPath, recordedRequest.getPath());
                assertEquals(HttpMethod.PUT.name(), recordedRequest.getMethod());
                return recordedRequest;
            }
        });
    }

    public void enqueuDataPackets(final String transactionPath, List<DataPacket> dataPackets, SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataPacketWriter dataPacketWriter;
        if (siteToSiteClientConfig.isUseCompression()) {
            dataPacketWriter = new DataPacketWriter(new CompressionOutputStream(byteArrayOutputStream));
        } else {
            dataPacketWriter = new DataPacketWriter(byteArrayOutputStream);
        }
        for (DataPacket dataPacket : dataPackets) {
            dataPacketWriter.write(dataPacket);
        }
        enqueue(new MockResponse().setBody(Long.toString(dataPacketWriter.close())), new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(transactionPath + "/flow-files", recordedRequest.getPath());
                assertEquals(HttpMethod.POST.name(), recordedRequest.getMethod());
                byte[] expecteds = byteArrayOutputStream.toByteArray();
                byte[] actuals = recordedRequest.getBody().readByteArray();
                if (!Arrays.equals(expecteds, actuals)) {
                    String message = null;
                    try {
                        message = "Expected:\n \"" + new String(expecteds, Charsets.UTF_8) + "\"\n but was\n \"" + new String(actuals, Charsets.UTF_8) + "\"";
                    } catch (Throwable throwable) {
                        fail("Expected " + Arrays.toString(expecteds) + " but was " + Arrays.toString(actuals));
                    }
                    fail(message);
                }
                assertEquals(HttpTransaction.APPLICATION_OCTET_STREAM, recordedRequest.getHeader(CONTENT_TYPE));
                assertEquals(HttpTransaction.TEXT_PLAIN, recordedRequest.getHeader(ACCEPT));
                return recordedRequest;
            }
        });
    }

    public void enqueueTransactionComplete(final String transactionPath, int flowFileSent, final ResponseCode requestResponseCode, ResponseCode replyResponseCode) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(TransactionResultParser.FLOW_FILE_SENT, flowFileSent);
        jsonObject.put(TransactionResultParser.RESPONSE_CODE, replyResponseCode.getCode());
        enqueue(new MockResponse().setBody(jsonObject.toString()), new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(transactionPath + "?responseCode=" + requestResponseCode.getCode(), recordedRequest.getPath());
                assertEquals(HttpMethod.DELETE.name(), recordedRequest.getMethod());
                assertEquals(HttpTransaction.APPLICATION_OCTET_STREAM, recordedRequest.getHeader(CONTENT_TYPE));
                return recordedRequest;
            }
        });
    }

    public void enqueue(MockResponse response, RequestAssertion requestAssertion) {
        mockWebServer.enqueue(response);
        requestAssertions.add(requestAssertion);
    }

    public void enqueueSiteToSitePeers(Collection<Peer> peers) throws JSONException, MalformedURLException {
        List<JSONObject> peerJsonObjects = new ArrayList<>(peers.size());
        for (Peer peer : peers) {
            peerJsonObjects.add(getPeerJSONObject(peer.isSecure(), peer.getHostname(), peer.getHttpPort(), peer.getFlowFileCount()));
        }
        enqueue(new MockResponse().setBody(getPeersJson(peerJsonObjects)), siteToSitePeerListRequestAssertion);
    }

    public void enqueueInputPorts(Map<String, String> nameToIdMap) throws JSONException {
        enqueue(new MockResponse().setBody(getPortIdentifierJSONObject(nameToIdMap).toString()), new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(HttpMethod.GET.name(), recordedRequest.getMethod());
                assertEquals(PeerTracker.NIFI_API_PATH + PeerTracker.SITE_TO_SITE_PATH, recordedRequest.getPath());
                return recordedRequest;
            }
        });
    }

    public String getPeersJson(Collection<JSONObject> peerJSONObjects) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        JSONArray jsonArray = new JSONArray(peerJSONObjects);
        jsonObject.put(PeerListParser.PEERS, jsonArray);
        return jsonObject.toString();
    }

    public JSONObject getPeerJSONObject(Boolean secure, String hostname, Integer port, Integer flowFileCount) throws JSONException {
        JSONObject jsonObject = new JSONObject();
        if (secure != null) {
            jsonObject.put(PeerListParser.SECURE, secure);
        }
        if (hostname != null) {
            jsonObject.put(PeerListParser.HOSTNAME, hostname);
        }
        if (port != null) {
            jsonObject.put(PeerListParser.PORT, port);
        }
        if (flowFileCount != null) {
            jsonObject.put(PeerListParser.FLOW_FILE_COUNT, flowFileCount);
        }
        return jsonObject;
    }

    public JSONObject getPortIdentifierJSONObject(Map<String, String> nameToIdMap) throws JSONException {
        JSONObject topLevelObject = new JSONObject();
        JSONObject controllerObject = new JSONObject();
        JSONArray inputPorts = new JSONArray();
        for (Map.Entry<String, String> entry : nameToIdMap.entrySet()) {
            JSONObject inputPortObject = new JSONObject();
            inputPortObject.put(SiteToSiteInfo.NAME, entry.getKey());
            inputPortObject.put(SiteToSiteInfo.ID, entry.getValue());
            inputPorts.put(inputPortObject);
        }
        controllerObject.put(SiteToSiteInfo.INPUT_PORTS, inputPorts);
        topLevelObject.put(SiteToSiteInfo.CONTROLLER, controllerObject);
        return topLevelObject;
    }

    public MockWebServer getMockWebServer() {
        return mockWebServer;
    }

    public void addRequestAssertion(RequestAssertion requestAssertion) {
        requestAssertions.add(requestAssertion);
    }
}
