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

import android.os.Parcel;
import android.os.Parcelable;

import org.apache.nifi.android.sitetosite.client.http.HttpSiteToSiteClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Configuration object for use with the @{@link HttpSiteToSiteClient}
 */
public class SiteToSiteClientConfig implements Parcelable {
    public static final Creator<SiteToSiteClientConfig> CREATOR = new Creator<SiteToSiteClientConfig>() {
        @Override
        public SiteToSiteClientConfig createFromParcel(Parcel source) {
            SiteToSiteClientConfig result = new SiteToSiteClientConfig();
            int remoteClusters = source.readInt();
            result.remoteClusters = new ArrayList<>(remoteClusters);
            for (int i = 0; i < remoteClusters; i++) {
                result.remoteClusters.add(source.<SiteToSiteRemoteCluster>readParcelable(SiteToSiteClientConfig.class.getClassLoader()));
            }
            result.timeoutNanos = source.readLong();
            result.idleConnectionExpirationNanos = source.readLong();
            result.useCompression = Boolean.valueOf(source.readString());
            result.portName = source.readString();
            result.portIdentifier = source.readString();
            result.preferredBatchDurationNanos = source.readLong();
            result.preferredBatchSize = source.readLong();
            result.preferredBatchCount = source.readInt();
            result.peerUpdateIntervalNanos = source.readLong();
            return result;
        }

        @Override
        public SiteToSiteClientConfig[] newArray(int size) {
            return new SiteToSiteClientConfig[size];
        }
    };

    private List<SiteToSiteRemoteCluster> remoteClusters = new ArrayList<>();
    private long timeoutNanos = TimeUnit.SECONDS.toNanos(30);
    private long idleConnectionExpirationNanos = TimeUnit.SECONDS.toNanos(30);
    private boolean useCompression;
    private String portName;
    private String portIdentifier;
    private long preferredBatchDurationNanos;
    private long preferredBatchSize;
    private int preferredBatchCount = 100;
    private long peerUpdateIntervalNanos = TimeUnit.MINUTES.toNanos(30);

    public SiteToSiteClientConfig() {

    }

    public SiteToSiteClientConfig(SiteToSiteClientConfig siteToSiteClientConfig) {
        List<SiteToSiteRemoteCluster> remoteClusters = siteToSiteClientConfig.getRemoteClusters();
        List<SiteToSiteRemoteCluster> remoteClustersCopy = new ArrayList<>(remoteClusters.size());
        for (SiteToSiteRemoteCluster siteToSiteRemoteCluster : remoteClusters) {
            remoteClustersCopy.add(new SiteToSiteRemoteCluster(siteToSiteRemoteCluster));
        }
        setRemoteClusters(remoteClustersCopy);
        this.timeoutNanos = siteToSiteClientConfig.getTimeout(TimeUnit.NANOSECONDS);
        this.idleConnectionExpirationNanos = siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.NANOSECONDS);
        this.useCompression = siteToSiteClientConfig.isUseCompression();
        this.portName = siteToSiteClientConfig.getPortName();
        this.portIdentifier = siteToSiteClientConfig.getPortIdentifier();
        this.preferredBatchDurationNanos = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.NANOSECONDS);
        this.preferredBatchSize = siteToSiteClientConfig.getPreferredBatchSize();
        this.preferredBatchCount = siteToSiteClientConfig.getPreferredBatchCount();
        this.peerUpdateIntervalNanos = siteToSiteClientConfig.getPeerUpdateInterval(TimeUnit.NANOSECONDS);
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(remoteClusters.size());
        for (SiteToSiteRemoteCluster remoteCluster : remoteClusters) {
            dest.writeParcelable(remoteCluster, 0);
        }
        dest.writeLong(timeoutNanos);
        dest.writeLong(idleConnectionExpirationNanos);
        dest.writeString(Boolean.toString(useCompression));
        dest.writeString(portName);
        dest.writeString(portIdentifier);
        dest.writeLong(preferredBatchDurationNanos);
        dest.writeLong(preferredBatchSize);
        dest.writeInt(preferredBatchCount);
        dest.writeLong(peerUpdateIntervalNanos);
    }

    public List<SiteToSiteRemoteCluster> getRemoteClusters() {
        return remoteClusters;
    }

    public void setRemoteClusters(List<SiteToSiteRemoteCluster> remoteClusters) {
        this.remoteClusters = remoteClusters;
    }

    /**
     * Gets the HTTP(S) connection timeout
     *
     * @param timeUnit time unit to get timeout in
     * @return the timeout
     */
    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Returns a boolean indicating whether compression will be used
     *
     * @return a boolean indicating whether compression will be used
     */
    public boolean isUseCompression() {
        return useCompression;
    }

    /**
     * Sets a boolean indicating whether compression will be used
     *
     * @param useCompression a boolean indicating whether compression will be used
     */
    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    /**
     * Gets the port name data will be sent to
     *
     * @return the port name data will be sent to
     */
    public String getPortName() {
        return portName;
    }

    /**
     * Sets the port name data will be sent to
     *
     * @param portName the port name data will be sent to
     */
    public void setPortName(String portName) {
        this.portName = portName;
    }

    /**
     * Gets the port identifier data will be sent to
     *
     * @return the port identifier data will be sent to
     */
    public String getPortIdentifier() {
        return portIdentifier;
    }

    /**
     * Sets the port identifier data will be sent to
     *
     * @param portIdentifier the port identifier data will be sent to
     */
    public void setPortIdentifier(String portIdentifier) {
        this.portIdentifier = portIdentifier;
    }

    /**
     * Gets the preferred batch duration
     *
     * @param timeUnit time unit to get duration for
     * @return the preferred batch duration
     */
    public long getPreferredBatchDuration(TimeUnit timeUnit) {
        return timeUnit.convert(preferredBatchDurationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Gets the preferred batch size
     *
     * @return the preferred batch size
     */
    public long getPreferredBatchSize() {
        return preferredBatchSize;
    }

    /**
     * Sets the preferred batch size
     *
     * @param preferredBatchSize the preferred batch size
     */
    public void setPreferredBatchSize(long preferredBatchSize) {
        this.preferredBatchSize = preferredBatchSize;
    }

    /**
     * Gets the preferred batch count
     *
     * @return the preferred batch count
     */
    public int getPreferredBatchCount() {
        return preferredBatchCount;
    }

    /**
     * Sets the preferred batch count
     *
     * @param preferredBatchCount the preferred batch count
     */
    public void setPreferredBatchCount(int preferredBatchCount) {
        this.preferredBatchCount = preferredBatchCount;
    }

    /**
     * Sets the HTTP(S) connection timeout
     *
     * @param timeout  the timeout
     * @param timeUnit the time unit
     */
    public void setTimeout(long timeout, TimeUnit timeUnit) {
        this.timeoutNanos = timeUnit.toNanos(timeout);
    }

    /**
     * Sets the preferred batch duration
     *
     * @param preferredBatchDuration the preferred batch duration
     * @param timeUnit               the time unit
     */
    public void setPreferredBatchDuration(long preferredBatchDuration, TimeUnit timeUnit) {
        this.preferredBatchDurationNanos = timeUnit.toNanos(preferredBatchDuration);
    }

    /**
     * Gets the peer update interval
     *
     * @param timeUnit the time unit
     * @return the peer update interval
     */
    public long getPeerUpdateInterval(TimeUnit timeUnit) {
        return timeUnit.convert(peerUpdateIntervalNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Sets the peer update interval
     *
     * @param peerUpdateInterval the peer update interval
     * @param timeUnit           the time unit
     */
    public void setPeerUpdateInterval(long peerUpdateInterval, TimeUnit timeUnit) {
        this.peerUpdateIntervalNanos = timeUnit.toNanos(peerUpdateInterval);
    }

    /**
     * Gets the idle connection expiration
     *
     * @param timeUnit the time unit
     * @return the idle connection expiration
     */
    public long getIdleConnectionExpiration(TimeUnit timeUnit) {
        return timeUnit.convert(idleConnectionExpirationNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * Sets the idle connection expiration
     *
     * @param idleConnectionExpiration the idle connection expiration
     * @param timeUnit                 the time unit
     */
    public void setIdleConnectionExpiration(long idleConnectionExpiration, TimeUnit timeUnit) {
        this.idleConnectionExpirationNanos = timeUnit.toNanos(idleConnectionExpiration);
    }

    public SiteToSiteClient createClient() throws IOException {
        return new SiteToSiteClient() {
            SiteToSiteRemoteCluster lastCluster = null;
            SiteToSiteClient lastClient = null;

            @Override
            public Transaction createTransaction() throws IOException {
                if (remoteClusters.size() == 0) {
                    throw new IOException("No remote clusters configured.");
                }
                IOException lastException = null;
                for (SiteToSiteRemoteCluster remoteCluster : remoteClusters) {
                    try {
                        SiteToSiteClient client;
                        if (remoteCluster.equals(lastCluster)) {
                            client = lastClient;
                        } else {
                            client = remoteCluster.getClientType().getFactory().create(SiteToSiteClientConfig.this, remoteCluster);
                        }
                        Transaction transaction = client.createTransaction();
                        lastClient = client;
                        lastCluster = remoteCluster;
                        return transaction;
                    } catch (IOException e) {
                        lastException = e;
                    }
                }
                throw lastException;
            }
        };
    }
}
