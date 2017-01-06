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

import org.apache.nifi.android.sitetosite.client.peer.PeerStatus;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SiteToSiteClientConfig implements Parcelable {
    public static final Creator<SiteToSiteClientConfig> CREATOR = new Creator<SiteToSiteClientConfig>() {
        @Override
        public SiteToSiteClientConfig createFromParcel(Parcel source) {
            SiteToSiteClientConfig result = new SiteToSiteClientConfig();
            List<String> urls = new ArrayList<>();
            source.readStringList(urls);
            result.urls = new HashSet<>(urls);
            result.timeoutNanos = source.readLong();
            result.idleConnectionExpirationNanos = source.readLong();
            result.keystoreFilename = source.readString();
            result.keystorePassword = source.readString();
            result.keystoreType = source.readString();
            result.truststoreFilename = source.readString();
            result.truststorePassword = source.readString();
            result.truststoreType = source.readString();
            result.useCompression = Boolean.valueOf(source.readString());
            result.portName = source.readString();
            result.portIdentifier = source.readString();
            result.preferredBatchDurationNanos = source.readLong();
            result.preferredBatchSize = source.readLong();
            result.preferredBatchCount = source.readInt();
            result.proxyHost = source.readString();
            result.proxyPort = source.readInt();
            result.proxyUsername = source.readString();
            result.proxyPassword = source.readString();
            result.peerUpdateIntervalNanos = source.readLong();
            result.peerStatus = source.readParcelable(SiteToSiteClientConfig.class.getClassLoader());
            result.username = source.readString();
            result.password = source.readString();
            return result;
        }

        @Override
        public SiteToSiteClientConfig[] newArray(int size) {
            return new SiteToSiteClientConfig[size];
        }
    };

    private Set<String> urls;
    private long timeoutNanos = TimeUnit.SECONDS.toNanos(30);
    private long idleConnectionExpirationNanos = TimeUnit.SECONDS.toNanos(30);
    private String keystoreFilename;
    private String keystorePassword;
    private String keystoreType;
    private String truststoreFilename;
    private String truststorePassword;
    private String truststoreType;
    private boolean useCompression;
    private String portName;
    private String portIdentifier;
    private long preferredBatchDurationNanos;
    private long preferredBatchSize;
    private int preferredBatchCount;
    private String proxyHost;
    private int proxyPort;
    private String proxyUsername;
    private String proxyPassword;
    private long peerUpdateIntervalNanos = TimeUnit.MINUTES.toNanos(30);
    private PeerStatus peerStatus;
    private String username;
    private String password;

    public SiteToSiteClientConfig() {

    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeStringList(urls == null ? new ArrayList<String>() : new ArrayList<>(urls));
        dest.writeLong(timeoutNanos);
        dest.writeLong(idleConnectionExpirationNanos);
        dest.writeString(keystoreFilename);
        dest.writeString(keystorePassword);
        dest.writeString(keystoreType);
        dest.writeString(truststoreFilename);
        dest.writeString(truststorePassword);
        dest.writeString(truststoreType);
        dest.writeString(Boolean.toString(useCompression));
        dest.writeString(portName);
        dest.writeString(portIdentifier);
        dest.writeLong(preferredBatchDurationNanos);
        dest.writeLong(preferredBatchSize);
        dest.writeInt(preferredBatchCount);
        dest.writeString(proxyHost);
        dest.writeInt(proxyPort);
        dest.writeString(proxyUsername);
        dest.writeString(proxyPassword);
        dest.writeLong(peerUpdateIntervalNanos);
        dest.writeParcelable(peerStatus, flags);
        dest.writeString(username);
        dest.writeString(password);
    }

    public String getUrl() {
        Set<String> urls = getUrls();
        if (urls == null || urls.size() == 0) {
            return null;
        }
        return urls.iterator().next();
    }

    public Set<String> getUrls() {
        return Collections.unmodifiableSet(urls);
    }

    public void setUrls(Collection<String> urls) {
        this.urls = new HashSet<>(urls);
    }

    public long getTimeout(TimeUnit timeUnit) {
        return timeUnit.convert(timeoutNanos, TimeUnit.NANOSECONDS);
    }

    public SSLContext getSslContext() {
        KeyManager[] keyManagers = getKeyManagers();
        TrustManager[] trustManagers = getTrustManagers();
        if (keyManagers != null || trustManagers != null) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(getKeyManagers(), trustManagers, null);
                sslContext.getDefaultSSLParameters().setNeedClientAuth(true);
                return sslContext;
            } catch (Exception e) {
                throw new IllegalStateException("Created keystore and truststore but failed to initialize SSLContext", e);
            }
        } else {
            return null;
        }
    }

    private KeyManager[] getKeyManagers() {
        String keystoreFilename = getKeystoreFilename();
        String keystorePassword = getKeystorePassword();
        String keystoreType = getKeystoreType();

        if (keystoreFilename != null && keystorePassword != null && keystoreType != null) {
            try {
                KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                KeyStore keystore = KeyStore.getInstance(keystoreType);
                loadKeystore(keystore, keystoreFilename, keystorePassword);
                keyManagerFactory.init(keystore, keystorePassword.toCharArray());
                return keyManagerFactory.getKeyManagers();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load Keystore", e);
            }
        } else {
            return null;
        }
    }

    private TrustManager[] getTrustManagers() {
        TrustManagerFactory trustManagerFactory;

        String truststoreFilename = getTruststoreFilename();
        String truststorePassword = getTruststorePassword();
        String truststoreType = getTruststoreType();

        if (truststoreFilename != null && truststorePassword != null && truststoreType != null) {
            try {
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                KeyStore trustStore = KeyStore.getInstance(truststoreType);
                loadKeystore(trustStore, truststoreFilename, truststorePassword);
                trustManagerFactory.init(trustStore);
                return trustManagerFactory.getTrustManagers();
            } catch (Exception e) {
                throw new IllegalStateException("Failed to load Truststore", e);
            }
        } else {
            return null;
        }
    }

    private void loadKeystore(KeyStore keystore, String filename, String password) throws IOException, GeneralSecurityException {
        Object e;
        if (filename.startsWith("classpath:")) {
            e = this.getClass().getClassLoader().getResourceAsStream(filename.substring("classpath:".length()));
        } else {
            e = new FileInputStream(filename);
        }

        try {
            keystore.load((InputStream) e, password.toCharArray());
        } finally {
            ((InputStream) e).close();
        }
    }

    public String getKeystoreFilename() {
        return keystoreFilename;
    }

    public void setKeystoreFilename(String keystoreFilename) {
        this.keystoreFilename = keystoreFilename;
    }

    public String getKeystorePassword() {
        return keystorePassword;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public String getKeystoreType() {
        return keystoreType;
    }

    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public String getTruststoreFilename() {
        return truststoreFilename;
    }

    public void setTruststoreFilename(String truststoreFilename) {
        this.truststoreFilename = truststoreFilename;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getTruststoreType() {
        return truststoreType;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
    }

    public boolean isUseCompression() {
        return useCompression;
    }

    public void setUseCompression(boolean useCompression) {
        this.useCompression = useCompression;
    }

    public String getPortName() {
        return portName;
    }

    public void setPortName(String portName) {
        this.portName = portName;
    }

    public String getPortIdentifier() {
        return portIdentifier;
    }

    public void setPortIdentifier(String portIdentifier) {
        this.portIdentifier = portIdentifier;
    }

    public long getPreferredBatchDuration(TimeUnit timeUnit) {
        return timeUnit.convert(preferredBatchDurationNanos, TimeUnit.NANOSECONDS);
    }

    public long getPreferredBatchSize() {
        return preferredBatchSize;
    }

    public void setPreferredBatchSize(long preferredBatchSize) {
        this.preferredBatchSize = preferredBatchSize;
    }

    public int getPreferredBatchCount() {
        return preferredBatchCount;
    }

    public void setPreferredBatchCount(int preferredBatchCount) {
        this.preferredBatchCount = preferredBatchCount;
    }

    public void setTimeoutNanos(long timeoutNanos) {
        this.timeoutNanos = timeoutNanos;
    }

    public void setPreferredBatchDurationNanos(long preferredBatchDurationNanos) {
        this.preferredBatchDurationNanos = preferredBatchDurationNanos;
    }

    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public String getProxyHost() {
        return proxyHost;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public long getPeerUpdateIntervalNanos() {
        return peerUpdateIntervalNanos;
    }

    public void setPeerUpdateIntervalNanos(long peerUpdateIntervalNanos) {
        this.peerUpdateIntervalNanos = peerUpdateIntervalNanos;
    }

    public PeerStatus getPeerStatus() {
        return peerStatus;
    }

    public void setPeerStatus(PeerStatus peerStatus) {
        this.peerStatus = peerStatus;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getIdleConnectionExpiration(TimeUnit timeUnit) {
        return timeUnit.convert(idleConnectionExpirationNanos, TimeUnit.NANOSECONDS);
    }

    public void setIdleConnectionExpirationNanos(long idleConnectionExpirationNanos) {
        this.idleConnectionExpirationNanos = idleConnectionExpirationNanos;
    }
}
