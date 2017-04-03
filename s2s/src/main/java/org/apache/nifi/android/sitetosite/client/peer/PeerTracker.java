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

import android.util.Log;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.apache.nifi.android.sitetosite.client.http.HttpPeerConnector;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Keeps track of peers and will try all of them in a prioritized order for any operations
 */
public class PeerTracker {
    public static final String CANONICAL_NAME = PeerTracker.class.getCanonicalName();
    public static final String NIFI_API_PATH = "/nifi-api";
    public static final String SITE_TO_SITE_PATH = "/site-to-site";

    private final Set<PeerKey> initialPeers;
    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final SiteToSiteRemoteCluster siteToSiteRemoteCluster;
    private final Map<PeerKey, HttpPeerConnector> peerConnectionManagerMap;
    private final PeerUpdater peerUpdater;
    private final Set<Thread> currentlyUpdating = new HashSet<>();
    private PeerStatus peerStatus;

    public PeerTracker(SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster, PeerUpdater peerUpdater) throws IOException {
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        this.siteToSiteRemoteCluster = siteToSiteRemoteCluster;
        this.initialPeers = new HashSet<>();
        this.peerConnectionManagerMap = new HashMap<>();
        List<Peer> peerList = new ArrayList<>();
        for (String initialPeer : siteToSiteRemoteCluster.getUrls()) {
            Peer peer = new Peer(initialPeer, 0);
            peerList.add(peer);
            this.initialPeers.add(peer.getPeerKey());
        }
        this.peerUpdater = peerUpdater;
        PeerStatus peerStatus = siteToSiteRemoteCluster.getPeerStatus();
        if (peerStatus == null) {
            this.peerStatus = new PeerStatus(peerList, 0L);
        } else {
            this.peerStatus = peerStatus;
        }
    }

    /**
     * Updates the peer list
     *
     * @throws IOException if no peer was able to send an updated peer list
     */
    public synchronized void updatePeers() throws IOException {
        long lastPeerUpdate = System.currentTimeMillis();
        List<Peer> newPeerList = peerUpdater.getPeers();
        Map<PeerKey, Peer> newPeerMap = new HashMap<>(newPeerList.size());
        for (Peer peer : newPeerList) {
            newPeerMap.put(peer.getPeerKey(), peer);
        }
        for (Peer oldPeer : peerStatus.getPeers()) {
            PeerKey oldPeerPeerKey = oldPeer.getPeerKey();
            Peer newPeer = newPeerMap.get(oldPeerPeerKey);
            if (newPeer != null) {
                oldPeer.setFlowFileCount(newPeer.getFlowFileCount());
                newPeerMap.put(oldPeerPeerKey, oldPeer);
            } else if (initialPeers.contains(oldPeerPeerKey)) {
                newPeerMap.put(oldPeerPeerKey, oldPeer);
            }
        }
        peerStatus = new PeerStatus(newPeerMap.values(), lastPeerUpdate);
        siteToSiteRemoteCluster.setPeerStatus(peerStatus);
    }

    public <O> O performHttpOperation(PeerOperation<O, HttpPeerConnector> operation) throws IOException {
        return performOperation(operation, new PeerConnectorFactory<HttpPeerConnector>(){
            @Override
            public HttpPeerConnector create(Peer peer) throws IOException {
                return getPeerConnectionManager(peer, siteToSiteClientConfig, siteToSiteRemoteCluster);
            }
        });
    }

    public <O> O performHttpOperation(Collection<Peer> peers, PeerOperation<O, HttpPeerConnector> operation) throws IOException {
        return performOperation(peers, operation, new PeerConnectorFactory<HttpPeerConnector>() {
            @Override
            public HttpPeerConnector create(Peer peer) throws IOException {
                return getPeerConnectionManager(peer, siteToSiteClientConfig, siteToSiteRemoteCluster);
            }
        });
    }

    public synchronized  <O, P> O performOperation(PeerOperation<O, P> operation, PeerConnectorFactory<P> connectorFactory) throws IOException {
        Thread thread = Thread.currentThread();
        if (currentlyUpdating.add(thread)) {
            try {
                updatePeersIfNecessary();
            } finally {
                currentlyUpdating.remove(thread);
            }
        }
        return performOperation(peerStatus.getPeers(), operation, connectorFactory);
    }

    public synchronized <O, P> O performOperation(Collection<Peer> peers, PeerOperation<O, P> operation, PeerConnectorFactory<P> connectorFactory) throws IOException {
        IOException lastException = null;
        for (Peer peer : peers) {
            try {
                P connectionManager = connectorFactory.create(peer);
                if (connectionManager == null) {
                    continue;
                }
                O result = operation.perform(peer, connectionManager);
                if (lastException != null) {
                    peerStatus.sort();
                }
                return result;
            } catch (IOException e) {
                peer.markFailure();
                Log.d(CANONICAL_NAME, "Unable to performHttpOperation operation to " + operation, e);
                lastException = e;
            }
        }
        if (lastException == null) {
            throw new IOException("No peers could be connected to with " + connectorFactory);
        }
        throw lastException;
    }

    /**
     * Gets the port identifier for a given port name
     *
     * @param portName the port name
     * @return the port identifier
     * @throws IOException if no peer was able to respond with s2s info
     */
    public String getPortIdentifier(final String portName) throws IOException {
        return performHttpOperation(new PeerOperation<String, HttpPeerConnector>() {
            @Override
            public String perform(Peer peer, HttpPeerConnector httpPeerConnector) throws IOException {
                String identifier = getSiteToSiteInfo(httpPeerConnector).getIdForInputPortName(portName);
                if (identifier == null) {
                    throw new IOException("Didn't find port named " + portName);
                }
                return identifier;
            }

            @Override
            public String toString() {
                return "get port identifier for name " + portName;
            }
        });
    }

    public SiteToSiteInfo getSiteToSiteInfo(HttpPeerConnector httpPeerConnector) throws IOException {
        HttpURLConnection httpURLConnection = httpPeerConnector.openConnection(SITE_TO_SITE_PATH);
        try {
            return new SiteToSiteInfo(httpURLConnection.getInputStream());
        } finally {
            httpURLConnection.disconnect();
        }
    }

    private void updatePeersIfNecessary() throws IOException {
        long currentTimeMillis = System.currentTimeMillis();
        if (currentTimeMillis - peerStatus.getLastPeerUpdate() > siteToSiteClientConfig.getPeerUpdateInterval(TimeUnit.MILLISECONDS)) {
            updatePeers();
        }
    }

    private HttpPeerConnector getPeerConnectionManager(Peer peer, SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster) {
        int httpPort = peer.getHttpPort();
        if (httpPort < 1 || httpPort > 65535) {
            return null;
        }
        PeerKey peerKey = peer.getPeerKey();
        HttpPeerConnector result = peerConnectionManagerMap.get(peerKey);
        if (result == null) {
            StringBuilder peerUrlBuilder = new StringBuilder("http");
            if (peer.isSecure()) {
                peerUrlBuilder.append("s");
            }
            peerUrlBuilder.append("://").append(peer.getHostname()).append(":").append(httpPort).append(NIFI_API_PATH);
            result = new HttpPeerConnector(peerUrlBuilder.toString(), siteToSiteClientConfig, siteToSiteRemoteCluster);
            peerConnectionManagerMap.put(peerKey, result);
        }
        return result;
    }
}
