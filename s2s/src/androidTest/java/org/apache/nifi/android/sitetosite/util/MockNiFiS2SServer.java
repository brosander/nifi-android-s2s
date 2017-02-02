package org.apache.nifi.android.sitetosite.util;

import org.apache.nifi.android.sitetosite.R;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.parser.TransactionResultParser;
import org.apache.nifi.android.sitetosite.client.protocol.CompressionOutputStream;
import org.apache.nifi.android.sitetosite.client.protocol.HttpMethod;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.client.transaction.DataPacketWriter;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class MockNiFiS2SServer {
    private final MockWebServer mockWebServer;
    private final List<RequestAssertion> requestAssertions;

    public MockNiFiS2SServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        requestAssertions = new ArrayList<>();
    }

    public String getNifiApiUrl() {
        return mockWebServer.url("/nifi-api").toString();
    }

    public List<RecordedRequest> verifyAssertions() throws Exception {
        assertEquals(requestAssertions.size(), mockWebServer.getRequestCount());
        List<RecordedRequest> result = new ArrayList<>(requestAssertions.size());
        for (RequestAssertion requestAssertion : requestAssertions) {
            result.add(requestAssertion.check());
        }
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
            mockResponse = mockResponse.addHeader(Transaction.LOCATION_URI_INTENT_NAME, Transaction.LOCATION_URI_INTENT_VALUE);
        }
        if (addLocationHeader) {
            mockResponse = mockResponse.addHeader(Transaction.LOCATION_HEADER_NAME, mockWebServer.url(transactionPath));
        }
        if (ttl != null) {
            mockResponse = mockResponse.addHeader(Transaction.SERVER_SIDE_TRANSACTION_TTL, ttl.toString());
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
                assertArrayEquals(byteArrayOutputStream.toByteArray(), recordedRequest.getBody().readByteArray());
                assertEquals(Transaction.APPLICATION_OCTET_STREAM, recordedRequest.getHeader(Transaction.CONTENT_TYPE));
                assertEquals(Transaction.TEXT_PLAIN, recordedRequest.getHeader(Transaction.ACCEPT));
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
                assertEquals(Transaction.APPLICATION_OCTET_STREAM, recordedRequest.getHeader(Transaction.CONTENT_TYPE));
                return recordedRequest;
            }
        });
    }
}
