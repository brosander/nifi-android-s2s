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

package org.apache.nifi.android.sitetosite.client.http;

import android.util.Base64;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;

import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.ACCEPT;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.AUTHORIZATION;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.CONTENT_TYPE;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.PROTOCOL_VERSION;
import static org.apache.nifi.android.sitetosite.client.http.HttpHeaders.PROXY_AUTHORIZATION;

/**
 * Manages connections with the NiFi peers
 */
public class HttpPeerConnector {
    public static final long THIRTY_SECONDS = TimeUnit.SECONDS.toMillis(30);

    private final String peerUrl;
    private final SiteToSiteClientConfig siteToSiteClientConfig;
    private final SiteToSiteRemoteCluster siteToSiteRemoteCluster;
    private final SSLSocketFactory socketFactory;
    private final Proxy proxy;
    private final String proxyAuth;
    private String authorization;
    private long authorizationExpiration;

    public HttpPeerConnector(String peerUrl, SiteToSiteClientConfig siteToSiteClientConfig, SiteToSiteRemoteCluster siteToSiteRemoteCluster) {
        this.peerUrl = peerUrl;
        this.siteToSiteClientConfig = siteToSiteClientConfig;
        this.siteToSiteRemoteCluster = siteToSiteRemoteCluster;
        SSLContext sslContext = siteToSiteRemoteCluster.getSslContext();
        if (sslContext != null) {
            socketFactory = sslContext.getSocketFactory();
        } else {
            socketFactory = null;
        }
        proxy = getProxy(siteToSiteRemoteCluster);
        String proxyUsername = siteToSiteRemoteCluster.getProxyUsername();
        if (proxy != null && proxyUsername != null && !proxyUsername.isEmpty()) {
            proxyAuth = siteToSiteRemoteCluster.getProxyAuthorizationType() + " " + Base64.encodeToString((proxyUsername + ":" + siteToSiteRemoteCluster.getProxyPassword()).getBytes(Charsets.ISO_8859_1), Base64.DEFAULT);
        } else {
            proxyAuth = null;
        }
    }

    /**
     * Url encodes parameters
     *
     * @param queryParameters the parameters
     * @return a urlencoded parameter string
     * @throws UnsupportedEncodingException
     */
    public static String urlEncodeParameters(Map<String, String> queryParameters) throws UnsupportedEncodingException {
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, String> entry : queryParameters.entrySet()) {
            stringBuilder.append(URLEncoder.encode(entry.getKey(), Charsets.UTF_8_NAME));
            stringBuilder.append("=");
            stringBuilder.append(URLEncoder.encode(entry.getValue(), Charsets.UTF_8_NAME));
            stringBuilder.append("&");
        }
        // Remove trailing &
        stringBuilder.setLength(stringBuilder.length() - 1);
        return stringBuilder.toString();
    }

    /**
     * Opens a connection to the given path
     *
     * @param path the path
     * @return an http connection
     * @throws IOException if there is a problem opening the connection
     */
    public HttpURLConnection openConnection(String path) throws IOException {
        return openConnection(path, new HashMap<String, String>());
    }

    /**
     * Opens a connection to the given path
     *
     * @param path the path
     * @param headers headers for the connection
     * @return an http connection
     * @throws IOException if there is a problem opening the connection
     */
    public HttpURLConnection openConnection(String path, Map<String, String> headers) throws IOException {
        return openConnection(path, headers, HttpMethod.GET);
    }

    /**
     * Opens a connection to the given path
     *
     * @param path the path
     * @param method the http method
     * @return an http connection
     * @throws IOException if there is a problem opening the connection
     */
    public HttpURLConnection openConnection(String path, HttpMethod method) throws IOException {
        return openConnection(path, new HashMap<String, String>(), method);
    }

    /**
     * Opens a connection to the given path
     *
     * @param path the path
     * @param headers headers for the connection
     * @param method the http method
     * @return an http connection
     * @throws IOException if there is a problem opening the connection
     */
    public HttpURLConnection openConnection(String path, Map<String, String> headers, HttpMethod method) throws IOException {
        return openConnection(path, headers, new HashMap<String, String>(), method);
    }

    /**
     * Opens a connection to the given path
     *
     * @param path the path
     * @param headers headers for the connection
     * @param queryParameters the query parameters
     * @param method the http method
     * @return an http connection
     * @throws IOException if there is a problem opening the connection
     */
    public HttpURLConnection openConnection(String path, Map<String, String> headers, Map<String, String> queryParameters, HttpMethod method) throws IOException {
        return openConnection(path, headers, queryParameters, method, false);
    }

    private HttpURLConnection openConnection(String path, Map<String, String> headers, Map<String, String> queryParameters, HttpMethod method, boolean isLogon) throws IOException {
        if (!isLogon) {
            loginIfNecessary();
        }
        String urlString = peerUrl + path;
        if (socketFactory != null && !urlString.startsWith("https://")) {
            throw new IOException("When keystore and/or truststore set, must use https");
        }

        String actualUrl = urlString;
        if (queryParameters.size() > 0) {
            actualUrl = urlString + "?" + urlEncodeParameters(queryParameters);
        }
        URL url = new URL(actualUrl);
        HttpURLConnection httpURLConnection;

        if (proxy == null) {
            httpURLConnection = (HttpURLConnection) url.openConnection();
        } else {
            httpURLConnection = (HttpURLConnection) url.openConnection(proxy);
        }

        if (socketFactory != null) {
            ((HttpsURLConnection)httpURLConnection).setSSLSocketFactory(socketFactory);
        }

        if (proxyAuth != null) {
            httpURLConnection.setRequestProperty(PROXY_AUTHORIZATION, proxyAuth);
        }

        if (!isLogon && authorization != null) {
            httpURLConnection.setRequestProperty(AUTHORIZATION, authorization);
        }

        int timeout = (int) siteToSiteClientConfig.getTimeout(TimeUnit.MILLISECONDS);
        httpURLConnection.setConnectTimeout(timeout);
        httpURLConnection.setReadTimeout(timeout);

        Map<String, String> finalHeaders = new HashMap<>(headers);

        if (!finalHeaders.containsKey(ACCEPT)) {
            finalHeaders.put(ACCEPT, "application/json");
        }

        if (!finalHeaders.containsKey(PROTOCOL_VERSION)) {
            finalHeaders.put(PROTOCOL_VERSION, "5");
        }

        for (Map.Entry<String, String> entry : finalHeaders.entrySet()) {
            httpURLConnection.setRequestProperty(entry.getKey(), entry.getValue());
        }

        httpURLConnection.setRequestMethod(method.name());

        return httpURLConnection;
    }

    private Proxy getProxy(SiteToSiteRemoteCluster siteToSiteRemoteCluster) {
        String proxyHost = siteToSiteRemoteCluster.getProxyHost();
        if (proxyHost == null || proxyHost.isEmpty()) {
            return null;
        }

        int proxyPort = siteToSiteRemoteCluster.getProxyPort();
        int port = 80;
        if (proxyPort <= 65535 && proxyPort > 0) {
            port = proxyPort;
        }
        return new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHost, port));
    }

    private void loginIfNecessary() throws IOException {
        long startTime = System.currentTimeMillis();
        if (startTime < authorizationExpiration) {
            return;
        }

        String username = siteToSiteRemoteCluster.getUsername();
        if (username == null || username.isEmpty()) {
            authorizationExpiration = Long.MAX_VALUE;
            return;
        }

        String password = siteToSiteRemoteCluster.getPassword();
        Map<String, String> map = new HashMap<>();
        map.put(ACCEPT, "text/plain");
        map.put(CONTENT_TYPE, "application/x-www-form-urlencoded");
        HttpURLConnection httpURLConnection = openConnection("/access/token", map, Collections.<String, String>emptyMap(), HttpMethod.POST, true);
        String payload = null;
        try {
            OutputStream outputStream = httpURLConnection.getOutputStream();
            try {
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(outputStream);
                try {
                    Map<String, String> formParams = new HashMap<>();
                    formParams.put("username", username);
                    formParams.put("password", password);
                    outputStreamWriter.write(urlEncodeParameters(formParams));
                } finally {
                    outputStreamWriter.close();
                }
            } finally {
                outputStream.close();
            }
            int responseCode = httpURLConnection.getResponseCode();
            if (responseCode < 200 || responseCode > 299) {
                throw new IOException("Got response code " + responseCode);
            }
            InputStream inputStream = httpURLConnection.getInputStream();
            byte[] buf = new byte[1024];
            StringBuilder stringBuilder = new StringBuilder();
            int read;
            while ((read = inputStream.read(buf, 0, buf.length)) != -1) {
                stringBuilder.append(new String(buf, 0, read, Charsets.UTF_8));
            }
            String token = stringBuilder.toString();
            authorization = "Bearer " + token;
            payload = new String(Base64.decode(token.split("\\.")[1], 0), Charsets.UTF_8);
            JSONObject jsonObject = new JSONObject(payload);
            long validDuration = (jsonObject.getLong("exp") - jsonObject.getLong("iat")) * 1000;
            if (validDuration < THIRTY_SECONDS) {
                throw new IOException("Authentication token valid duration is < " + THIRTY_SECONDS);
            }
            authorizationExpiration = startTime + validDuration - THIRTY_SECONDS;
        } catch (JSONException e) {
            throw new IOException("Received unparseable token payload: " + payload, e);
        } finally {
            httpURLConnection.disconnect();
        }
    }
}
