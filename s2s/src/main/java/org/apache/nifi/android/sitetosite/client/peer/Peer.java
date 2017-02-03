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

/**
 * Information useful when determining which peer to send data to
 */
public class Peer implements Comparable<Peer>, Parcelable {
    private final String url;
    private int flowFileCount;
    private long lastFailure = 0L;

    public static final Creator<Peer> CREATOR = new Creator<Peer>() {
        @Override
        public Peer createFromParcel(Parcel source) {
            return new Peer(source.readString(), source.readInt(), source.readLong());
        }

        @Override
        public Peer[] newArray(int size) {
            return new Peer[size];
        }
    };

    public Peer(String url, int flowFileCount) {
        this(url, flowFileCount, 0L);
    }

    public Peer(String url, int flowFileCount, long lastFailure) {
        this.url = url;
        this.flowFileCount = flowFileCount;
        this.lastFailure = lastFailure;
    }

    /**
     * Gets the url of the peer
     *
     * @return the url of the peeer
     */
    public String getUrl() {
        return url;
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
        lastFailure = SystemClock.elapsedRealtime();
    }

    @Override
    public int compareTo(Peer o) {
        if (lastFailure > o.lastFailure) {
            return 1;
        } else if (lastFailure < o.lastFailure) {
            return -1;
        } else if (flowFileCount < o.flowFileCount) {
            return -1;
        } else if (flowFileCount > o.flowFileCount) {
            return 1;
        }
        return url.compareTo(o.url);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Peer peer = (Peer) o;

        if (flowFileCount != peer.flowFileCount) return false;
        if (lastFailure != peer.lastFailure) return false;
        return url != null ? url.equals(peer.url) : peer.url == null;

    }

    @Override
    public int hashCode() {
        int result = url != null ? url.hashCode() : 0;
        result = 31 * result + flowFileCount;
        result = 31 * result + (int) (lastFailure ^ (lastFailure >>> 32));
        return result;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(url);
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

    @Override
    public String toString() {
        return "Peer{" +
                "url='" + url + '\'' +
                ", flowFileCount=" + flowFileCount +
                ", lastFailure=" + lastFailure +
                '}';
    }
}
