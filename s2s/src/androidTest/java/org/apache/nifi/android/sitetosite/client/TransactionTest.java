package org.apache.nifi.android.sitetosite.client;

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.peer.PeerConnectionManager;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.apache.nifi.android.sitetosite.util.MockScheduledExecutor;
import org.apache.nifi.android.sitetosite.util.MockNiFiS2SServer;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TransactionTest {
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
            assertNull(recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_USE_COMPRESSION));
            assertEquals(Long.toString(siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION));
            assertNull(recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_COUNT));
            assertNull(recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_SIZE));
            assertNull(recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_DURATION));
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
            assertEquals(Boolean.toString(true), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_USE_COMPRESSION));
            assertEquals(Long.toString(siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_REQUEST_EXPIRATION));
            assertEquals(Integer.toString(siteToSiteClientConfig.getPreferredBatchCount()), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_COUNT));
            assertEquals(Long.toString(siteToSiteClientConfig.getPreferredBatchSize()), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_SIZE));
            assertEquals(Long.toString(siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.MILLISECONDS)), recordedRequest.getHeader(Transaction.HANDSHAKE_PROPERTY_BATCH_DURATION));
        }
    }

    @Test
    public void testNoTransactionUrlIntent() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(Transaction.EXPECTED_TRANSACTION_URL_AS_INTENT);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, null, 30);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new Transaction(new PeerConnectionManager(new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0), siteToSiteClientConfig), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testExpectedTransactionUrl() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(Transaction.EXPECTED_TRANSACTION_URL);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30, false);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new Transaction(new PeerConnectionManager(new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0), siteToSiteClientConfig), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testExpectedTtl() throws Exception {
        expectedException.expect(IOException.class);
        expectedException.expectMessage(Transaction.EXPECTED_TTL);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, null);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new Transaction(new PeerConnectionManager(new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0), siteToSiteClientConfig), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    @Test
    public void testUnparseableTtl() throws Exception {
        expectedException.expect(IOException.class);
        String ttl = "abcd";
        expectedException.expectMessage(Transaction.UNABLE_TO_PARSE_TTL + ttl);

        mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, ttl);
        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        new Transaction(new PeerConnectionManager(new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0), siteToSiteClientConfig), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        mockNiFiS2SServer.verifyAssertions();
    }

    private List<RecordedRequest> performTestSuccessfulTransaction(SiteToSiteClientConfig siteToSiteClientConfig) throws Exception {
        String transactionPath = mockNiFiS2SServer.enqueuCreateTransaction(portIdentifier, transactionIdentifier, 30);

        mockNiFiS2SServer.enqueueTtlExtension(transactionPath);

        List<DataPacket> dataPackets = Arrays.<DataPacket>asList(new ByteArrayDataPacket(new HashMap<String, String>(), "testMessage".getBytes(Charsets.UTF_8)));
        mockNiFiS2SServer.enqueuDataPackets(transactionPath, dataPackets, siteToSiteClientConfig);

        mockNiFiS2SServer.enqueueTransactionComplete(transactionPath, dataPackets.size(), ResponseCode.CONFIRM_TRANSACTION, ResponseCode.CONFIRM_TRANSACTION);
        Transaction transaction = new Transaction(new PeerConnectionManager(new Peer(mockNiFiS2SServer.getNifiApiUrl(), 0), siteToSiteClientConfig), portIdentifier, siteToSiteClientConfig, scheduledThreadPoolExecutor);
        scheduledThreadPoolExecutor.getTtlExtender(15).run();

        for (DataPacket dataPacket : dataPackets) {
            transaction.send(dataPacket);
        }
        transaction.confirm();
        transaction.complete();
        return mockNiFiS2SServer.verifyAssertions();
    }
}
