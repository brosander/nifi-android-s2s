package org.apache.nifi.android.sitetosite.util;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.http.HttpMethod;
import org.apache.nifi.android.sitetosite.client.http.HttpTransaction;
import org.apache.nifi.android.sitetosite.client.http.parser.PeerListParser;
import org.apache.nifi.android.sitetosite.client.http.parser.TransactionResultParser;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.peer.PeerTracker;
import org.apache.nifi.android.sitetosite.client.peer.SiteToSiteInfo;
import org.apache.nifi.android.sitetosite.client.protocol.CompressionOutputStream;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.client.transaction.DataPacketWriter;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
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

import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.ACCEPT;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.LOCATION_HEADER_NAME;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.LOCATION_URI_INTENT_NAME;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.LOCATION_URI_INTENT_VALUE;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.SERVER_SIDE_TRANSACTION_TTL;
import static org.apache.nifi.android.sitetosite.client.http.HttpSiteToSiteClient.SITE_TO_SITE_PEERS_PATH;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class MockNiFiS2SServer {
    private final MockWebServer mockWebServer;
    private final List<RequestAssertion> requestAssertions;
    private int verifyIndex = 0;

    public MockNiFiS2SServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        requestAssertions = new ArrayList<>();
    }

    public String getNifiApiUrl() {
        return mockWebServer.url("/nifi-api").toString();
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
        mockWebServer.enqueue(mockResponse);

        requestAssertions.add(new RequestAssertion() {
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
        mockWebServer.enqueue(new MockResponse());

        requestAssertions.add(new RequestAssertion() {
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
        mockWebServer.enqueue(new MockResponse().setBody(Long.toString(dataPacketWriter.close())));

        requestAssertions.add(new RequestAssertion() {
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
        mockWebServer.enqueue(new MockResponse().setBody(jsonObject.toString()));

        requestAssertions.add(new RequestAssertion() {
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

    public void enqueueSiteToSitePeers(Collection<Peer> peers) throws JSONException, MalformedURLException {
        List<JSONObject> peerJsonObjects = new ArrayList<>(peers.size());
        for (Peer peer : peers) {
            peerJsonObjects.add(getPeerJSONObject(peer.isSecure(), peer.getHostname(), peer.getHttpPort(), peer.getFlowFileCount()));
        }
        mockWebServer.enqueue(new MockResponse().setBody(getPeersJson(peerJsonObjects)));

        requestAssertions.add(new RequestAssertion() {
            @Override
            public RecordedRequest check() throws Exception {
                RecordedRequest recordedRequest = mockWebServer.takeRequest();
                assertEquals(PeerTracker.NIFI_API_PATH + SITE_TO_SITE_PEERS_PATH, recordedRequest.getPath());
                return recordedRequest;
            }
        });
    }

    public void enqueueInputPorts(Map<String, String> nameToIdMap) throws JSONException {
        mockWebServer.enqueue(new MockResponse().setBody(getPortIdentifierJSONObject(nameToIdMap).toString()));

        requestAssertions.add(new RequestAssertion() {
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
