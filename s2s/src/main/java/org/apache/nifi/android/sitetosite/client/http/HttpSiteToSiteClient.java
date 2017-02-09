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

package org.apache.nifi.android.sitetosite.client.http;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.http.parser.PeerListParser;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.peer.PeerOperation;
import org.apache.nifi.android.sitetosite.client.peer.PeerTracker;
import org.apache.nifi.android.sitetosite.client.peer.PeerUpdater;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static org.apache.nifi.android.sitetosite.client.peer.PeerTracker.SITE_TO_SITE_PATH;

/**
 * Client class for sending data to NiFi over s2s
 */
public class HttpSiteToSiteClient implements PeerUpdater, SiteToSiteClient {
    public static final String SITE_TO_SITE_PEERS_PATH = SITE_TO_SITE_PATH + "/peers";
    public static final String RECEIVED_RESPONSE_CODE = "Received response code ";
    public static final String WHEN_OPENING = " when opening ";

    private final PeerTracker peerTracker;
    private final String portIdentifier;
    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final ScheduledExecutorService ttlExtendTaskExecutor;

    public HttpSiteToSiteClient(SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        peerTracker = new PeerTracker(siteToSiteClientConfig, this);

        String portIdentifier = siteToSiteClientConfig.getPortIdentifier();
        if (portIdentifier == null) {
            this.portIdentifier = peerTracker.getPortIdentifier(siteToSiteClientConfig.getPortName());
            siteToSiteClientConfig.setPortIdentifier(this.portIdentifier);
        } else {
            this.portIdentifier = portIdentifier;
        }

        ttlExtendTaskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName(Thread.currentThread().getName() + " TTLExtend");
                thread.setDaemon(true);
                return thread;
            }
        });
    }

    @Override
    public HttpTransaction createTransaction() throws IOException {
        return peerTracker.performHttpOperation(new PeerOperation<HttpTransaction, HttpPeerConnector>() {
            @Override
            public HttpTransaction perform(Peer peer, HttpPeerConnector httpPeerConnector) throws IOException {
                return new HttpTransaction(httpPeerConnector, portIdentifier, siteToSiteClientConfig, ttlExtendTaskExecutor);
            }

            @Override
            public String toString() {
                return "create transaction for port " + portIdentifier;
            }
        });
    }

    @Override
    public List<Peer> getPeers() throws IOException {
        return peerTracker.performHttpOperation(new PeerOperation<List<Peer>, HttpPeerConnector>() {
            @Override
            public List<Peer> perform(Peer peer, HttpPeerConnector httpPeerConnector) throws IOException {
                HttpURLConnection httpURLConnection = httpPeerConnector.openConnection(SITE_TO_SITE_PEERS_PATH);
                try {
                    int responseCode = httpURLConnection.getResponseCode();
                    if (responseCode < 200 || responseCode > 299) {
                        throw new IOException(RECEIVED_RESPONSE_CODE + responseCode + WHEN_OPENING + httpURLConnection.getURL());
                    }
                    return PeerListParser.parsePeers(httpURLConnection.getInputStream());
                } finally {
                    httpURLConnection.disconnect();
                }
            }
        });
    }

    public String getPortIdentifier() {
        return portIdentifier;
    }
}
