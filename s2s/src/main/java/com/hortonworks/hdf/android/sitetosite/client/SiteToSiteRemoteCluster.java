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

package com.hortonworks.hdf.android.sitetosite.client;

import android.os.Parcel;
import android.os.Parcelable;

import com.hortonworks.hdf.android.sitetosite.client.http.HttpSiteToSiteClient;
import com.hortonworks.hdf.android.sitetosite.client.peer.PeerStatus;
import com.hortonworks.hdf.android.sitetosite.client.socket.SocketSiteToSiteClient;
import com.hortonworks.hdf.android.sitetosite.util.SerializationUtils;

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

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

public class SiteToSiteRemoteCluster implements Parcelable {
    public enum ClientType {
        HTTP(new SiteToSiteClientFactory(){
            @Override
            public SiteToSiteClient create(SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster) throws IOException {
                return new HttpSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);
            }
        }, "HTTP(S)"),
        RAW(new SiteToSiteClientFactory(){
            @Override
            public SiteToSiteClient create(SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster) throws IOException {
                return new SocketSiteToSiteClient(siteToSiteClientConfig, siteToSiteRemoteCluster);
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

        public SiteToSiteClientFactory getFactory() {
            return factory;
        }
    }

    private Set<String> urls;
    private String keystoreFilename;
    private String keystorePassword;
    private String keystoreType;
    private String truststoreFilename;
    private String truststorePassword;
    private String truststoreType;
    private String proxyHost;
    private int proxyPort;
    private String proxyAuthorizationType = "Basic";
    private String proxyUsername;
    private String proxyPassword;
    private PeerStatus peerStatus;
    private String username;
    private String password;
    private ClientType clientType = ClientType.HTTP;

    public static final Creator<SiteToSiteRemoteCluster> CREATOR = new Creator<SiteToSiteRemoteCluster>() {
        @Override
        public SiteToSiteRemoteCluster createFromParcel(Parcel source) {
            SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();
            List<String> urls = new ArrayList<>();
            source.readStringList(urls);
            siteToSiteRemoteCluster.urls = new HashSet<>(urls);
            siteToSiteRemoteCluster.keystoreFilename = source.readString();
            siteToSiteRemoteCluster.keystorePassword = source.readString();
            siteToSiteRemoteCluster.keystoreType = source.readString();
            siteToSiteRemoteCluster.truststoreFilename = source.readString();
            siteToSiteRemoteCluster.truststorePassword = source.readString();
            siteToSiteRemoteCluster.truststoreType = source.readString();
            siteToSiteRemoteCluster.proxyHost = source.readString();
            siteToSiteRemoteCluster.proxyPort = source.readInt();
            siteToSiteRemoteCluster.proxyAuthorizationType = source.readString();
            siteToSiteRemoteCluster.proxyUsername = source.readString();
            siteToSiteRemoteCluster.proxyPassword = source.readString();
            siteToSiteRemoteCluster.peerStatus = source.readParcelable(SiteToSiteRemoteCluster.class.getClassLoader());
            siteToSiteRemoteCluster.username = source.readString();
            siteToSiteRemoteCluster.password = source.readString();
            siteToSiteRemoteCluster.clientType = ClientType.valueOf(source.readString());
            return siteToSiteRemoteCluster;
        }

        @Override
        public SiteToSiteRemoteCluster[] newArray(int size) {
            return new SiteToSiteRemoteCluster[size];
        }
    };

    public SiteToSiteRemoteCluster() {

    }

    public SiteToSiteRemoteCluster(SiteToSiteRemoteCluster siteToSiteRemoteCluster) {
        this.urls = new HashSet<>(siteToSiteRemoteCluster.getUrls());
        this.keystoreFilename = siteToSiteRemoteCluster.keystoreFilename;
        this.keystorePassword = siteToSiteRemoteCluster.keystorePassword;
        this.keystoreType = siteToSiteRemoteCluster.keystoreType;
        this.truststoreFilename = siteToSiteRemoteCluster.truststoreFilename;
        this.truststorePassword = siteToSiteRemoteCluster.truststorePassword;
        this.truststoreType = siteToSiteRemoteCluster.truststoreType;
        this.proxyHost = siteToSiteRemoteCluster.getProxyHost();
        this.proxyPort = siteToSiteRemoteCluster.getProxyPort();
        this.proxyAuthorizationType = siteToSiteRemoteCluster.getProxyAuthorizationType();
        this.proxyUsername = siteToSiteRemoteCluster.getProxyUsername();
        this.proxyPassword = siteToSiteRemoteCluster.getProxyPassword();
        this.peerStatus = SerializationUtils.unmarshallParcelable(SerializationUtils.marshallParcelable(siteToSiteRemoteCluster.getPeerStatus()), PeerStatus.class);
        this.username = siteToSiteRemoteCluster.getUsername();
        this.password = siteToSiteRemoteCluster.getPassword();
        this.clientType = siteToSiteRemoteCluster.getClientType();
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
     * Gets the proxy authorization type
     *
     * @return the proxy authorization type
     */
    public String getProxyAuthorizationType() {
        return proxyAuthorizationType;
    }

    /**
     * Sets the proxy authorization type
     *
     * @param proxyAuthorizationType the proxy authorization type
     */
    public void setProxyAuthorizationType(String proxyAuthorizationType) {
        this.proxyAuthorizationType = proxyAuthorizationType;
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

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeStringList(new ArrayList<>(urls));
        dest.writeString(keystoreFilename);
        dest.writeString(keystorePassword);
        dest.writeString(keystoreType);
        dest.writeString(truststoreFilename);
        dest.writeString(truststorePassword);
        dest.writeString(truststoreType);
        dest.writeString(proxyHost);
        dest.writeInt(proxyPort);
        dest.writeString(proxyAuthorizationType);
        dest.writeString(proxyUsername);
        dest.writeString(proxyPassword);
        dest.writeParcelable(peerStatus, 0);
        dest.writeString(username);
        dest.writeString(password);
        dest.writeString(clientType.name());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SiteToSiteRemoteCluster that = (SiteToSiteRemoteCluster) o;

        if (proxyPort != that.proxyPort) return false;
        if (urls != null ? !urls.equals(that.urls) : that.urls != null) return false;
        if (keystoreFilename != null ? !keystoreFilename.equals(that.keystoreFilename) : that.keystoreFilename != null)
            return false;
        if (keystorePassword != null ? !keystorePassword.equals(that.keystorePassword) : that.keystorePassword != null)
            return false;
        if (keystoreType != null ? !keystoreType.equals(that.keystoreType) : that.keystoreType != null)
            return false;
        if (truststoreFilename != null ? !truststoreFilename.equals(that.truststoreFilename) : that.truststoreFilename != null)
            return false;
        if (truststorePassword != null ? !truststorePassword.equals(that.truststorePassword) : that.truststorePassword != null)
            return false;
        if (truststoreType != null ? !truststoreType.equals(that.truststoreType) : that.truststoreType != null)
            return false;
        if (proxyHost != null ? !proxyHost.equals(that.proxyHost) : that.proxyHost != null)
            return false;
        if (proxyAuthorizationType != null ? !proxyAuthorizationType.equals(that.proxyAuthorizationType) : that.proxyAuthorizationType != null)
            return false;
        if (proxyUsername != null ? !proxyUsername.equals(that.proxyUsername) : that.proxyUsername != null)
            return false;
        if (proxyPassword != null ? !proxyPassword.equals(that.proxyPassword) : that.proxyPassword != null)
            return false;
        if (peerStatus != null ? !peerStatus.equals(that.peerStatus) : that.peerStatus != null)
            return false;
        if (username != null ? !username.equals(that.username) : that.username != null)
            return false;
        if (password != null ? !password.equals(that.password) : that.password != null)
            return false;
        return clientType == that.clientType;

    }

    @Override
    public int hashCode() {
        int result = urls != null ? urls.hashCode() : 0;
        result = 31 * result + (keystoreFilename != null ? keystoreFilename.hashCode() : 0);
        result = 31 * result + (keystorePassword != null ? keystorePassword.hashCode() : 0);
        result = 31 * result + (keystoreType != null ? keystoreType.hashCode() : 0);
        result = 31 * result + (truststoreFilename != null ? truststoreFilename.hashCode() : 0);
        result = 31 * result + (truststorePassword != null ? truststorePassword.hashCode() : 0);
        result = 31 * result + (truststoreType != null ? truststoreType.hashCode() : 0);
        result = 31 * result + (proxyHost != null ? proxyHost.hashCode() : 0);
        result = 31 * result + proxyPort;
        result = 31 * result + (proxyAuthorizationType != null ? proxyAuthorizationType.hashCode() : 0);
        result = 31 * result + (proxyUsername != null ? proxyUsername.hashCode() : 0);
        result = 31 * result + (proxyPassword != null ? proxyPassword.hashCode() : 0);
        result = 31 * result + (peerStatus != null ? peerStatus.hashCode() : 0);
        result = 31 * result + (username != null ? username.hashCode() : 0);
        result = 31 * result + (password != null ? password.hashCode() : 0);
        result = 31 * result + (clientType != null ? clientType.hashCode() : 0);
        return result;
    }
}
