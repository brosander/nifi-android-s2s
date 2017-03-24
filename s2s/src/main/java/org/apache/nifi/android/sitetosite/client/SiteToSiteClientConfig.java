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
import org.apache.nifi.android.sitetosite.client.peer.PeerStatus;
import org.apache.nifi.android.sitetosite.client.socket.SocketSiteToSiteClient;
import org.apache.nifi.android.sitetosite.util.SerializationUtils;

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

/**
 * Configuration object for use with the @{@link HttpSiteToSiteClient}
 */
public class SiteToSiteClientConfig implements Parcelable {
    public enum ClientType {
        HTTP(new SiteToSiteClientFactory(){
            @Override
            public SiteToSiteClient create(SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
                return new HttpSiteToSiteClient(siteToSiteClientConfig);
            }
        }, "HTTP(S)"),
        RAW(new SiteToSiteClientFactory(){
            @Override
            public SiteToSiteClient create(SiteToSiteClientConfig siteToSiteClientConfig) throws IOException {
                return new SocketSiteToSiteClient(siteToSiteClientConfig);
            }
        }, "RAW");

        private final SiteToSiteClientFactory factory;
        private final String displayName;


        ClientType(SiteToSiteClientFactory factory, String displayName) {
            this.factory = factory;
            this.displayName = displayName;
        }

        public String getDisplayName() {
            return displayName;
        }
    }
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
            result.clientType = ClientType.valueOf(source.readString());
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
    private ClientType clientType = ClientType.HTTP;

    public SiteToSiteClientConfig() {

    }

    public SiteToSiteClientConfig(SiteToSiteClientConfig siteToSiteClientConfig) {
        this.urls = siteToSiteClientConfig.getUrls();
        this.timeoutNanos = siteToSiteClientConfig.getTimeout(TimeUnit.NANOSECONDS);
        this.idleConnectionExpirationNanos = siteToSiteClientConfig.getIdleConnectionExpiration(TimeUnit.NANOSECONDS);
        this.keystoreFilename = siteToSiteClientConfig.keystoreFilename;
        this.keystorePassword = siteToSiteClientConfig.keystorePassword;
        this.keystoreType = siteToSiteClientConfig.keystoreType;
        this.truststoreFilename = siteToSiteClientConfig.truststoreFilename;
        this.truststorePassword = siteToSiteClientConfig.truststorePassword;
        this.truststoreType = siteToSiteClientConfig.truststoreType;
        this.useCompression = siteToSiteClientConfig.isUseCompression();
        this.portName = siteToSiteClientConfig.getPortName();
        this.portIdentifier = siteToSiteClientConfig.getPortIdentifier();
        this.preferredBatchDurationNanos = siteToSiteClientConfig.getPreferredBatchDuration(TimeUnit.NANOSECONDS);
        this.preferredBatchSize = siteToSiteClientConfig.getPreferredBatchSize();
        this.preferredBatchCount = siteToSiteClientConfig.getPreferredBatchCount();
        this.proxyHost = siteToSiteClientConfig.getProxyHost();
        this.proxyPort = siteToSiteClientConfig.getProxyPort();
        this.proxyUsername = siteToSiteClientConfig.getProxyUsername();
        this.proxyPassword = siteToSiteClientConfig.getProxyPassword();
        this.peerUpdateIntervalNanos = siteToSiteClientConfig.getPeerUpdateInterval(TimeUnit.NANOSECONDS);
        this.peerStatus = SerializationUtils.unmarshallParcelable(SerializationUtils.marshallParcelable(siteToSiteClientConfig.getPeerStatus()), PeerStatus.class);
        this.username = siteToSiteClientConfig.getUsername();
        this.password = siteToSiteClientConfig.getPassword();
        this.clientType = siteToSiteClientConfig.getClientType();
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
        dest.writeString(clientType.name());
    }

    /**
     * Gets the NiFi instance URLs
     *
     * @return the NiFi instance URLs
     */
    public Set<String> getUrls() {
        return Collections.unmodifiableSet(urls);
    }

    /**
     * Sets the NiFi instance URLs
     *
     * @param urls the NiFi instance URLs
     */
    public void setUrls(Collection<String> urls) {
        this.urls = new HashSet<>(urls);
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
     * Gets the ssl context for use making the connections
     *
     * @return the ssl context
     */
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
        if (truststoreFilename != null && truststorePassword != null && truststoreType != null) {
            try {
                TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
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

    public void setKeystoreFilename(String keystoreFilename) {
        this.keystoreFilename = keystoreFilename;
    }

    public void setKeystorePassword(String keystorePassword) {
        this.keystorePassword = keystorePassword;
    }

    public void setKeystoreType(String keystoreType) {
        this.keystoreType = keystoreType;
    }

    public void setTruststoreFilename(String truststoreFilename) {
        this.truststoreFilename = truststoreFilename;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public void setTruststoreType(String truststoreType) {
        this.truststoreType = truststoreType;
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
     * Gets the proxy host
     *
     * @return the proxy host
     */
    public String getProxyHost() {
        return proxyHost;
    }

    /**
     * Sets the proxy host
     *
     * @param proxyHost the proxy host
     */
    public void setProxyHost(String proxyHost) {
        this.proxyHost = proxyHost;
    }

    /**
     * Gets the proxy port
     *
     * @return the proxy port
     */
    public int getProxyPort() {
        return proxyPort;
    }

    /**
     * Sets the proxy port
     *
     * @param proxyPort the proxy port
     */
    public void setProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
    }

    /**
     * Gets the proxy username
     *
     * @return the proxy username
     */
    public String getProxyUsername() {
        return proxyUsername;
    }

    /**
     * Sets the proxy username
     *
     * @param proxyUsername the proxy username
     */
    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    /**
     * Gets the proxy password
     *
     * @return the proxy password
     */
    public String getProxyPassword() {
        return proxyPassword;
    }

    /**
     * Sets the proxy password
     *
     * @param proxyPassword the proxy password
     */
    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
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
     * Gets the peer status
     *
     * @return the peer status
     */
    public PeerStatus getPeerStatus() {
        return peerStatus;
    }

    /**
     * Sets the peer status
     *
     * @param peerStatus the peer status
     */
    public void setPeerStatus(PeerStatus peerStatus) {
        this.peerStatus = peerStatus;
    }

    /**
     * Gets the username
     *
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * Sets the username
     *
     * @param username the username
     */
    public void setUsername(String username) {
        this.username = username;
    }

    /**
     * Gets the password
     *
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password
     *
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
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
     * Gets the client type
     *
     * @return the client type
     */
    public ClientType getClientType() {
        return clientType;
    }

    /**
     * Sets the client type
     *
     * @param clientType the client type
     */
    public void setClientType(ClientType clientType) {
        this.clientType = clientType;
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
        return clientType.factory.create(this);
    }
}
