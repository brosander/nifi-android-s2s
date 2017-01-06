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

package org.apache.nifi.android.sitetosite.client.peer;

import android.os.SystemClock;
import android.util.Log;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.parser.PeerListParser;
import org.apache.nifi.android.sitetosite.client.parser.PortIdentifierParser;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class PeerTracker {
    public static final String CANONICAL_NAME = PeerTracker.class.getCanonicalName();
    private final Set<String> initialPeers;
    private final ScheduledExecutorService ttlExtendTaskExecutor;
    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final Map<String, PeerConnectionManager> peerConnectionManagerMap;
    private PeerStatus peerStatus;

    public PeerTracker(Set<String> initialPeers, SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        this.initialPeers = new HashSet<>();
        this.peerConnectionManagerMap = new HashMap<>();
        List<Peer> peerList = new ArrayList<>();
        for (String initialPeer : initialPeers) {
            URL url = new URL(initialPeer);
            String peerUrl = url.getProtocol() + "://" + url.getHost() + ":" + url.getPort() + "/nifi-api";
            this.initialPeers.add(peerUrl);
            peerList.add(new Peer(peerUrl, 0));
        }

        PeerStatus peerStatus = siteToSiteClientConfig.getPeerStatus();
        if (peerStatus == null) {
            this.peerStatus = new PeerStatus(peerList, 0L);
            updatePeers();
        } else if (peerStatus.getLastPeerUpdate() > SystemClock.elapsedRealtime()) {
            // Device has been restarted
            this.peerStatus = new PeerStatus(peerStatus.getPeers(), 0L);
            updatePeers();
        } else {
            this.peerStatus = peerStatus;
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

    public synchronized void updatePeers() throws IOException {
        IOException lastException = null;
        long lastPeerUpdate = SystemClock.elapsedRealtime();
        for (Peer peer : peerStatus.getPeers()) {
            try {
                HttpURLConnection httpURLConnection = getPeerConnectionManager(peer).openConnection("/site-to-site/peers");
                try {
                    int responseCode = httpURLConnection.getResponseCode();
                    if (responseCode < 200 || responseCode > 299) {
                        throw new IOException("Received response code " + responseCode + " when opening " + peer.getUrl());
                    }
                    Map<String, Peer> newPeerMap = PeerListParser.parsePeers(httpURLConnection.getInputStream());
                    if (newPeerMap != null) {
                        for (Peer oldPeer : peerStatus.getPeers()) {
                            String url = oldPeer.getUrl();
                            Peer newPeer = newPeerMap.get(url);
                            if (newPeer != null) {
                                oldPeer.setFlowFileCount(newPeer.getFlowFileCount());
                                newPeerMap.put(url, oldPeer);
                            } else if (initialPeers.contains(url)) {
                                newPeerMap.put(url, oldPeer);
                            }
                        }
                        peerStatus = new PeerStatus(newPeerMap.values(), lastPeerUpdate);
                        siteToSiteClientConfig.setPeerStatus(peerStatus);
                        return;
                    }
                } finally {
                    httpURLConnection.disconnect();
                }
            } catch (IOException e) {
                Log.d(CANONICAL_NAME, "Unable to get peer list from " + peer.getUrl(), e);
                lastException = e;
            }
        }
        if (lastException != null) {
            throw lastException;
        }
    }

    public synchronized Transaction createTransaction(String portIdentifier) throws IOException {
        IOException lastException = null;
        updatePeersIfNecessary();
        for (Peer peer : peerStatus.getPeers()) {
            String peerUrl = peer.getUrl();
            try {
                Transaction transaction = new Transaction(getPeerConnectionManager(peer), portIdentifier, siteToSiteClientConfig, ttlExtendTaskExecutor);
                if (lastException != null) {
                    peerStatus.sort();
                }
                return transaction;
            } catch (IOException e) {
                peer.markFailure();
                Log.d(CANONICAL_NAME, "Unable to create transaction for port " + portIdentifier + " to peer " + peerUrl);
                lastException = e;
            }
        }
        throw lastException;
    }

    public synchronized String getPortIdentifier(String portName) throws IOException {
        IOException lastException = null;
        updatePeersIfNecessary();
        for (Peer peer : peerStatus.getPeers()) {
            HttpURLConnection httpURLConnection = getPeerConnectionManager(peer).openConnection("/site-to-site");
            try {
                String identifier = PortIdentifierParser.getPortIdentifier(httpURLConnection.getInputStream(), portName);
                if (identifier == null) {
                    throw new IOException("Didn't find port named " + portName);
                }
                if (lastException != null) {
                    peerStatus.sort();
                }
                return identifier;
            } catch (IOException e) {
                peer.markFailure();
                Log.d(CANONICAL_NAME, "Unable to get port identifier from " + peer.getUrl());
                lastException = e;
            } finally {
                httpURLConnection.disconnect();
            }
        }
        throw lastException;
    }

    private void updatePeersIfNecessary() throws IOException {
        long elapsedRealtime = SystemClock.elapsedRealtime();
        if (TimeUnit.NANOSECONDS.convert(elapsedRealtime - peerStatus.getLastPeerUpdate(), TimeUnit.MILLISECONDS) > siteToSiteClientConfig.getPeerUpdateIntervalNanos()) {
            updatePeers();
        }
    }

    private PeerConnectionManager getPeerConnectionManager(Peer peer) {
        String peerUrl = peer.getUrl();
        PeerConnectionManager result = peerConnectionManagerMap.get(peerUrl);
        if (result == null) {
            result = new PeerConnectionManager(peer, siteToSiteClientConfig);
            peerConnectionManagerMap.put(peerUrl, result);
        }
        return result;
    }
}
