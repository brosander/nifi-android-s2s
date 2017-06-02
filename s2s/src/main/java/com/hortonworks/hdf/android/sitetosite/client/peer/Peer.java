/*
 * Copyright 2017 Hortonworks, Inc.
 * All rights reserved.
 *
 *   Hortonworks, Inc. licenses this file to you under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * See the associated NOTICE file for additional information regarding copyright ownership.
 */

package com.hortonworks.hdf.android.sitetosite.client.peer;

import android.os.Parcel;
import android.os.Parcelable;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Information useful when determining which peer to send data to
 */
public class Peer implements Comparable<Peer>, Parcelable {
    private final String hostname;
    private final int httpPort;
    private final int rawPort;
    private final boolean secure;
    private int flowFileCount;
    private long lastFailure = 0L;

    public static final Creator<Peer> CREATOR = new Creator<Peer>() {
        @Override
        public Peer createFromParcel(Parcel source) {
            return new Peer(source.readString(), source.readInt(), source.readInt(), Boolean.valueOf(source.readString()), source.readInt(), source.readLong());
        }

        @Override
        public Peer[] newArray(int size) {
            return new Peer[size];
        }
    };

    public Peer(String urlString, int flowFileCount) throws MalformedURLException {
        URL url = new URL(urlString);
        this.hostname = url.getHost();
        this.httpPort = url.getPort();
        this.rawPort = 0;
        this.secure = url.getProtocol().equals("https");
        this.flowFileCount = flowFileCount;
        this.lastFailure = 0L;
    }

    public Peer(String hostname, int httpPort, int rawPort, boolean secure, int flowFileCount) {
        this(hostname, httpPort, rawPort, secure, flowFileCount, 0L);
    }

    public Peer(String hostname, int httpPort, int rawPort, boolean secure, int flowFileCount, long lastFailure) {
        this.hostname = hostname;
        this.httpPort = httpPort;
        this.rawPort = rawPort;
        this.secure = secure;
        this.flowFileCount = flowFileCount;
        this.lastFailure = lastFailure;
    }

    /**
     * Gets the current flow file count of the peer
     *
     * @return the current flow file count of the peer
     */
    public int getFlowFileCount() {
        return flowFileCount;
    }

    public void setFlowFileCount(int flowFileCount) {
        this.flowFileCount = flowFileCount;
    }

    /**
     * Marks that there was a failure communicating with the peer
     */
    public void markFailure() {
        lastFailure = System.currentTimeMillis();
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(hostname);
        dest.writeInt(httpPort);
        dest.writeInt(rawPort);
        dest.writeString(Boolean.toString(secure));
        dest.writeInt(flowFileCount);
        dest.writeLong(lastFailure);
    }

    /**
     * Gets the last failure timestamp
     *
     * @return the last failure timestamp
     */
    public long getLastFailure() {
        return lastFailure;
    }

    public String getHostname() {
        return hostname;
    }

    public int getHttpPort() {
        return httpPort;
    }

    public boolean isSecure() {
        return secure;
    }

    public int getRawPort() {
        return rawPort;
    }

    public int compareTo(Peer o) {
        if (lastFailure > o.lastFailure) {
            return 1;
        } else if (lastFailure < o.lastFailure) {
            return -1;
        } else if (flowFileCount < o.flowFileCount) {
            return -1;
        } else if (flowFileCount > o.flowFileCount) {
            return 1;
        } else {
            int hostCompare = hostname.compareTo(o.hostname);
            if (hostCompare != 0) {
                return hostCompare;
            }
            int httpPortCompare = httpPort - o.httpPort;
            if (httpPortCompare != 0) {
                return hostCompare;
            }
            int rawPortCompare = rawPort - o.rawPort;
            if (rawPortCompare != 0) {
                return rawPortCompare;
            }
            return (secure ? 1 : 0) - (o.secure? 1 : 0);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Peer peer = (Peer) o;

        if (httpPort != peer.httpPort) return false;
        if (secure != peer.secure) return false;
        if (rawPort != peer.rawPort) return false;
        if (flowFileCount != peer.flowFileCount) return false;
        if (lastFailure != peer.lastFailure) return false;
        return hostname != null ? hostname.equals(peer.hostname) : peer.hostname == null;

    }

    @Override
    public int hashCode() {
        int result = hostname != null ? hostname.hashCode() : 0;
        result = 31 * result + httpPort;
        result = 31 * result + (secure ? 1 : 0);
        result = 31 * result + rawPort;
        result = 31 * result + flowFileCount;
        result = 31 * result + (int) (lastFailure ^ (lastFailure >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return "Peer{" +
                "hostname='" + hostname + '\'' +
                ", httpPort=" + httpPort +
                ", secure=" + secure +
                ", rawPort=" + rawPort +
                ", flowFileCount=" + flowFileCount +
                ", lastFailure=" + lastFailure +
                '}';
    }

    public PeerKey getPeerKey() {
        return new PeerKey(this);
    }
}
