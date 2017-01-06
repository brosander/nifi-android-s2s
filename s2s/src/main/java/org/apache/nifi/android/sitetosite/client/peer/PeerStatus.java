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

import android.os.Parcel;
import android.os.Parcelable;
import android.os.SystemClock;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class PeerStatus implements Parcelable {
    private final List<Peer> peers;
    private final long lastPeerUpdate;

    public static final Creator<PeerStatus> CREATOR = new Creator<PeerStatus>() {
        @Override
        public PeerStatus createFromParcel(Parcel source) {
            int numPeers = source.readInt();
            List<Peer> peers = new ArrayList<>(numPeers);
            for (int i = 0; i < numPeers; i++) {
                peers.add(source.<Peer>readParcelable(PeerStatus.class.getClassLoader()));
            }
            return new PeerStatus(peers, source.readLong());
        }

        @Override
        public PeerStatus[] newArray(int size) {
            return new PeerStatus[size];
        }
    };

    public PeerStatus(Collection<Peer> peers, long lastPeerUpdate) {
        this.peers = new ArrayList<>(peers.size());
        for (Peer peer : peers) {
            if (peer.getLastFailure() > SystemClock.elapsedRealtime()) {
                this.peers.add(new Peer(peer.getUrl(), peer.getFlowFileCount()));
            } else {
                this.peers.add(peer);
            }
        }
        this.lastPeerUpdate = lastPeerUpdate;
        sort();
    }

    public PeerStatus(PeerStatus peerStatus) {
        this(peerStatus.getPeers(), peerStatus.getLastPeerUpdate());
    }

    public List<Peer> getPeers() {
        return peers;
    }

    public void sort() {
        Collections.sort(peers);
    }

    public long getLastPeerUpdate() {
        return lastPeerUpdate;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(peers.size());
        for (Peer peer : peers) {
            dest.writeParcelable(peer, flags);
        }
        dest.writeLong(lastPeerUpdate);
    }
}
