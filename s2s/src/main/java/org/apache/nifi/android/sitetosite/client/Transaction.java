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

package org.apache.nifi.android.sitetosite.client;

import android.util.Log;

import org.apache.nifi.android.sitetosite.client.parser.TransactionResultParser;
import org.apache.nifi.android.sitetosite.client.peer.PeerConnectionManager;
import org.apache.nifi.android.sitetosite.client.protocol.CompressionOutputStream;
import org.apache.nifi.android.sitetosite.client.protocol.HttpMethod;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.IOUtils;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

public class Transaction {
    public static final String CANONICAL_NAME = Transaction.class.getCanonicalName();

    public static final String LOCATION_HEADER_NAME = "Location";
    public static final String LOCATION_URI_INTENT_NAME = "x-location-uri-intent";
    public static final String LOCATION_URI_INTENT_VALUE = "transaction-url";

    public static final String SERVER_SIDE_TRANSACTION_TTL = "x-nifi-site-to-site-server-transaction-ttl";

    public static final String HANDSHAKE_PROPERTY_USE_COMPRESSION = "x-nifi-site-to-site-use-compression";
    public static final String HANDSHAKE_PROPERTY_REQUEST_EXPIRATION = "x-nifi-site-to-site-request-expiration";
    public static final String HANDSHAKE_PROPERTY_BATCH_COUNT = "x-nifi-site-to-site-batch-count";
    public static final String HANDSHAKE_PROPERTY_BATCH_SIZE = "x-nifi-site-to-site-batch-size";
    public static final String HANDSHAKE_PROPERTY_BATCH_DURATION = "x-nifi-site-to-site-batch-duration";

    private static final Map<String, String> BEGIN_TRANSACTION_HEADERS = initBeginTransactionHeaders();
    private static final Map<String, String> END_TRANSACTION_HEADERS = initEndTransactionHeaders();
    private static final Pattern NIFI_API_PATTERN = Pattern.compile(Pattern.quote("/nifi-api"));

    private final Map<String, String> handshakeProperties;
    private final String transactionUrl;
    private final PeerConnectionManager peerConnectionManager;
    private final CRC32 crc;
    private final OutputStream sendFlowFilesOutputStream;
    private final HttpURLConnection sendFlowFilesConnection;
    private final ScheduledFuture<?> ttlExtendFuture;

    public Transaction(PeerConnectionManager peerConnectionManager, String portIdentifier, SiteToSiteClientConfig siteToSiteClientConfig, ScheduledExecutorService ttlExtendTaskExecutor) throws IOException {
        this.peerConnectionManager = peerConnectionManager;
        this.handshakeProperties = createHandshakeProperties(siteToSiteClientConfig);

        HttpURLConnection createTransactionConnection = peerConnectionManager.openConnection("/data-transfer/input-ports/" + portIdentifier + "/transactions", handshakeProperties, HttpMethod.POST);
        int responseCode = createTransactionConnection.getResponseCode();
        if (responseCode < 200 || responseCode > 299) {
            throw new IOException("Got response code " + responseCode);
        }

        int ttl;
        if (LOCATION_URI_INTENT_VALUE.equals(createTransactionConnection.getHeaderField(LOCATION_URI_INTENT_NAME))) {
            String ttlString = createTransactionConnection.getHeaderField(SERVER_SIDE_TRANSACTION_TTL);
            if (ttlString == null || ttlString.isEmpty()) {
                throw new IOException("Expected " + SERVER_SIDE_TRANSACTION_TTL + " header");
            } else {
                try {
                    ttl = Integer.parseInt(ttlString);
                } catch (Exception e) {
                    throw new IOException("Unable to parse " + SERVER_SIDE_TRANSACTION_TTL + " as int: " + ttlString, e);
                }
            }
            String path = new URL(createTransactionConnection.getHeaderField(LOCATION_HEADER_NAME)).getPath();
            transactionUrl = NIFI_API_PATTERN.matcher(path).replaceFirst("");
        } else {
            throw new IOException("Expected header " + LOCATION_URI_INTENT_NAME + " == " + LOCATION_URI_INTENT_VALUE);
        }

        crc = new CRC32();
        Map<String, String> beginTransactionHeaders = new HashMap<>(BEGIN_TRANSACTION_HEADERS);
        beginTransactionHeaders.putAll(handshakeProperties);
        sendFlowFilesConnection = peerConnectionManager.openConnection(transactionUrl + "/flow-files", beginTransactionHeaders, HttpMethod.POST);
        OutputStream outputStream = sendFlowFilesConnection.getOutputStream();
        if (siteToSiteClientConfig.isUseCompression()) {
            outputStream = new CompressionOutputStream(outputStream);
        }
        outputStream = new CheckedOutputStream(outputStream, crc);
        this.sendFlowFilesOutputStream = outputStream;
        ttlExtendFuture = ttlExtendTaskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    HttpURLConnection ttlExtendConnection = Transaction.this.peerConnectionManager.openConnection(transactionUrl, handshakeProperties, HttpMethod.PUT);
                    try {
                        int responseCode = ttlExtendConnection.getResponseCode();
                        if (responseCode < 200 || responseCode > 299) {
                            Log.w(CANONICAL_NAME, "Extending ttl failed for transaction (responseCode " + responseCode + ")" + transactionUrl);
                        }
                    } finally {
                        ttlExtendConnection.disconnect();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }, ttl / 2, ttl / 2, TimeUnit.SECONDS);
    }

    private Map<String, String> createHandshakeProperties(SiteToSiteClientConfig siteToSiteClientConfig) {
        Map<String, String> handshakeProperties = new HashMap<>();

        if (siteToSiteClientConfig.isUseCompression()) {
            handshakeProperties.put(HANDSHAKE_PROPERTY_USE_COMPRESSION, "true");
        }

        long requestExpirationMillis = siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.MILLISECONDS);
        if (requestExpirationMillis > 0) {
            handshakeProperties.put(HANDSHAKE_PROPERTY_REQUEST_EXPIRATION, String.valueOf(requestExpirationMillis));
        }

        int batchCount = siteToSiteClientConfig.getPreferredBatchCount();
        if (batchCount > 0) {
            handshakeProperties.put(HANDSHAKE_PROPERTY_BATCH_COUNT, String.valueOf(batchCount));
        }

        long batchSize = siteToSiteClientConfig.getPreferredBatchSize();
        if (batchSize > 0) {
            handshakeProperties.put(HANDSHAKE_PROPERTY_BATCH_SIZE, String.valueOf(batchSize));
        }

        long batchDurationMillis = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.MILLISECONDS);
        if (batchDurationMillis > 0) {
            handshakeProperties.put(HANDSHAKE_PROPERTY_BATCH_DURATION, String.valueOf(batchDurationMillis));
        }

        return Collections.unmodifiableMap(handshakeProperties);
    }

    private static Map<String, String> initEndTransactionHeaders() {
        Map<String, String> result = new HashMap<>();
        result.put("Content-Type", "application/octet-stream");
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, String> initBeginTransactionHeaders() {
        Map<String, String> result = new HashMap<>();
        result.put("Content-Type", "application/octet-stream");
        result.put("Accept", "text/plain");
        return Collections.unmodifiableMap(result);
    }

    public void send(DataPacket dataPacket) throws IOException {
        final DataOutputStream out = new DataOutputStream(sendFlowFilesOutputStream);

        final Map<String, String> attributes = dataPacket.getAttributes();
        out.writeInt(attributes.size());
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            writeString(entry.getKey(), out);
            writeString(entry.getValue(), out);
        }

        out.writeLong(dataPacket.getSize());

        final InputStream in = dataPacket.getData();
        byte[] buf = new byte[1024];
        int read = 0;
        while ((read = in.read(buf)) != -1) {
            out.write(buf, 0, read);
        }
        out.flush();
    }

    private void writeString(final String val, final DataOutputStream out) throws IOException {
        final byte[] bytes = val.getBytes("UTF-8");
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public void confirm() throws IOException {
        int responseCode = sendFlowFilesConnection.getResponseCode();
        if (responseCode != 200 && responseCode != 202) {
            throw new IOException("Got response code " + responseCode);
        }
        long calculatedCrc = crc.getValue();
        long serverCrc = IOUtils.readInputStreamAndParseAsLong(sendFlowFilesConnection.getInputStream());
        if (calculatedCrc != serverCrc) {
            endTransaction(ResponseCode.BAD_CHECKSUM);
            throw new IOException("Should have " + calculatedCrc + " for crc, got " + serverCrc);
        }
    }

    public TransactionResult complete() throws IOException {
        return endTransaction(ResponseCode.CONFIRM_TRANSACTION);
    }

    public TransactionResult cancel() throws IOException {
        return endTransaction(ResponseCode.CANCEL_TRANSACTION);
    }

    private TransactionResult endTransaction(ResponseCode responseCodeToSend) throws IOException {
        ttlExtendFuture.cancel(false);
        try {
            ttlExtendFuture.get();
        } catch (Exception e) {
            if (!(e instanceof CancellationException)) {
                throw new IOException("Error waiting on ttl extension thread to end.", e);
            }
        }
        sendFlowFilesConnection.disconnect();
        Map<String, String> queryParameters = new HashMap<>();
        queryParameters.put("responseCode", Integer.toString(responseCodeToSend.getCode()));
        Map<String, String> endTransactionHeaders = new HashMap<>(END_TRANSACTION_HEADERS);
        endTransactionHeaders.putAll(handshakeProperties);
        HttpURLConnection delete = peerConnectionManager.openConnection(transactionUrl, endTransactionHeaders, queryParameters, HttpMethod.DELETE);
        try {
            int responseCode = delete.getResponseCode();
            if (responseCode < 200 || responseCode > 299) {
                throw new IOException("Got response code " + responseCode);
            }
            InputStream inputStream = delete.getInputStream();
            try {
                return TransactionResultParser.parseTransactionResult(inputStream);
            } finally {
                inputStream.close();
            }
        } finally {
            delete.disconnect();
        }
    }
}
