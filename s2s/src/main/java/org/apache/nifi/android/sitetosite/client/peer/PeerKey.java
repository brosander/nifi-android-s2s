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

public class PeerKey {
    private final String hostname;
    private final int httpPort;
    private final int rawPort;
    private final boolean secure;

    public PeerKey(Peer peer) {
        this(peer.getHostname(), peer.getHttpPort(), peer.getRawPort(), peer.isSecure());
    }

    public PeerKey(String hostname, int httpPort, int rawPort, boolean secure) {
        this.hostname = hostname;
        this.httpPort = httpPort;
        this.rawPort = rawPort;
        this.secure = secure;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PeerKey peerKey = (PeerKey) o;

        if (httpPort != peerKey.httpPort) return false;
        if (rawPort != peerKey.rawPort) return false;
        if (secure != peerKey.secure) return false;
        return hostname != null ? hostname.equals(peerKey.hostname) : peerKey.hostname == null;

    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + httpPort;
        result = 31 * result + rawPort;
        result = 31 * result + (secure ? 1 : 0);
        return result;
    }
}
