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

package org.apache.nifi.android.sitetosite.client.socket;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.http.HttpPeerConnector;
import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.client.peer.PeerConnectorFactory;
import org.apache.nifi.android.sitetosite.client.peer.PeerOperation;
import org.apache.nifi.android.sitetosite.client.peer.PeerTracker;
import org.apache.nifi.android.sitetosite.client.peer.PeerUpdater;
import org.apache.nifi.android.sitetosite.client.peer.SiteToSiteInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SocketSiteToSiteClient implements PeerUpdater, SiteToSiteClient {
    public static final PeerConnectorFactory<SocketPeerConnector> CONNECTOR_FACTORY = new PeerConnectorFactory<SocketPeerConnector>() {
        @Override
        public SocketPeerConnector create(Peer peer, SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
            int rawPort = peer.getRawPort();
            if (rawPort <= 0 || rawPort > 65535) {
                return null;
            }
            return new SocketPeerConnector(peer, siteToSiteClientConfig);
        }
    };

    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final PeerTracker peerTracker;

    public SocketSiteToSiteClient(SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        peerTracker = new PeerTracker(siteToSiteClientConfig, this);

        peerTracker.performHttpOperation(new PeerOperation<Void, HttpPeerConnector>() {
            @Override
            public Void perform(Peer peer, HttpPeerConnector connectionManager) throws IOException {
                initPortIdentifier(connectionManager);
                return null;
            }
        });
    }

    @Override
    public List<Peer> getPeers() throws IOException {
        try {
            return peerTracker.performHttpOperation(new PeerOperation<List<Peer>, HttpPeerConnector>() {
                @Override
                public List<Peer> perform(Peer peer, HttpPeerConnector httpPeerConnector) throws IOException {
                    int rawPort = peer.getRawPort();
                    Peer rawPeer = peer;
                    SiteToSiteInfo siteToSiteInfo = initPortIdentifier(httpPeerConnector);
                    if (rawPort <= 0 || rawPort > 65535) {
                        if (siteToSiteInfo == null) {
                            siteToSiteInfo = peerTracker.getSiteToSiteInfo(httpPeerConnector);
                        }
                        Integer rawSiteToSitePort = siteToSiteInfo.getRawSiteToSitePort();
                        if (rawSiteToSitePort == null) {
                            throw new IOException("Instance is not configured for raw site to site");
                        }
                        rawPeer = new Peer(peer.getHostname(), peer.getHttpPort(), rawSiteToSitePort, peer.isSecure(), peer.getFlowFileCount(), peer.getLastFailure());
                    }
                    List<Peer> result = new ArrayList<>();
                    result.add(rawPeer);
                    result.addAll(peerTracker.performOperation(Collections.singletonList(rawPeer), new SocketGetPeersPeerOperation(), CONNECTOR_FACTORY));
                    return result;
                }
            });
        } catch (IOException e) {
            return peerTracker.performOperation(new SocketGetPeersPeerOperation(), CONNECTOR_FACTORY);
        }
    }

    @Override
    public SocketTransaction createTransaction() throws IOException {
        return peerTracker.performOperation(new PeerOperation<SocketTransaction, SocketPeerConnector>() {
            @Override
            public SocketTransaction perform(Peer peer, SocketPeerConnector connectionManager) throws IOException {
                return new SocketTransaction(connectionManager.openConnection(true), siteToSiteClientConfig);
            }
        }, CONNECTOR_FACTORY);
    }

    private SiteToSiteInfo initPortIdentifier(HttpPeerConnector httpPeerConnector) throws IOException {
        if (siteToSiteClientConfig.getPortIdentifier() != null) {
            return null;
        }
        SiteToSiteInfo siteToSiteInfo = peerTracker.getSiteToSiteInfo(httpPeerConnector);
        siteToSiteClientConfig.setPortIdentifier(siteToSiteInfo.getIdForInputPortName(siteToSiteClientConfig.getPortName()));
        return siteToSiteInfo;
    }
}
