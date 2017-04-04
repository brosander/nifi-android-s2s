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

package org.apache.nifi.android.sitetosite.factory;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteRemoteCluster;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PropertiesSiteToSiteClientConfigFactoryTest<T extends PropertiesSiteToSiteClientConfigFactory> {
    private PropertiesSiteToSiteClientConfigFactory propertiesSiteToSiteClientConfigFactory;

    @Before
    public void setup() {
        propertiesSiteToSiteClientConfigFactory = initFactory();
    }

    protected PropertiesSiteToSiteClientConfigFactory initFactory() {
        return new PropertiesSiteToSiteClientConfigFactory();
    }

    @Test
    public void testNoRemoteClusters() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(0, load("").getRemoteClusters().size());
    }

    @Test
    public void testRemoteClusters() throws IOException, SiteToSiteClientConfigCreationException, NoSuchFieldException, IllegalAccessException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);

        String remote0Url0 = "https://nifi.remote.0.url.0:8443/nifi-api";
        String remote0Url1 = "https://nifi.remote.0.url.1:8443/nifi-api";
        String remote0Keystore = "remote0Keystore.p12";
        String remote0KeystorePassword = "remote0KeystorePassword";
        String remote0KeystoreType = "PKCS12";
        String remote0Truststore = "remote0Truststore.bks";
        String remote0TruststorePassword = "remote0TruststorePassword";
        String remote0TruststoreType = "BKS";
        String remote0ProxyHost = "remote0ProxyHost";
        int remote0ProxyPort = 1111;
        String remote0ProxyUsername = "remote0ProxyUsername";
        String remote0ProxyPassword = "remote0ProxyPassword";
        SiteToSiteRemoteCluster.ClientType remote0ClientType = SiteToSiteRemoteCluster.ClientType.RAW;

        String remote1Url0 = "https://nifi.remote.1.url.0:8443/nifi-api";
        String remote1Truststore = "remote1Truststore.p12";
        String remote1TruststorePassword = "remote1TruststorePassword";
        String remote1TruststoreType = "PKCS12";
        String remote1Username = "remote0Username";
        String remote1Password = "remote0Password";

        try {
            String baseProp0 = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG_REMOTE + "0.";
            String baseProp1 = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG_REMOTE + "1.";

            writeLine(bufferedWriter, baseProp0 + "url.0=" + remote0Url0);
            writeLine(bufferedWriter, baseProp0 + "url.1=" + remote0Url1);

            writeLine(bufferedWriter, baseProp0 + "keystore=" + remote0Keystore);
            writeLine(bufferedWriter, baseProp0 + "keystorePasswd=" + remote0KeystorePassword);
            writeLine(bufferedWriter, baseProp0 + "keystoreType=" + remote0KeystoreType);

            writeLine(bufferedWriter, baseProp0 + "truststore=" + remote0Truststore);
            writeLine(bufferedWriter, baseProp0 + "truststorePasswd=" + remote0TruststorePassword);
            writeLine(bufferedWriter, baseProp0 + "truststoreType=" + remote0TruststoreType);

            writeLine(bufferedWriter, baseProp0 + "proxyHost=" + remote0ProxyHost);
            writeLine(bufferedWriter, baseProp0 + "proxyPort=" + remote0ProxyPort);
            writeLine(bufferedWriter, baseProp0 + "proxyUsername=" + remote0ProxyUsername);
            writeLine(bufferedWriter, baseProp0 + "proxyPassword=" + remote0ProxyPassword);

            writeLine(bufferedWriter, baseProp0 + "clientType=" + remote0ClientType.name());

            writeLine(bufferedWriter, baseProp1 + "url.0=" + remote1Url0);
            writeLine(bufferedWriter, baseProp1 + "truststore=" + remote1Truststore);
            writeLine(bufferedWriter, baseProp1 + "truststorePasswd=" + remote1TruststorePassword);
            writeLine(bufferedWriter, baseProp1 + "truststoreType=" + remote1TruststoreType);

            writeLine(bufferedWriter, baseProp1 + "username=" + remote1Username);
            writeLine(bufferedWriter, baseProp1 + "password=" + remote1Password);
        } finally {
            bufferedWriter.close();
            stringWriter.close();
        }

        SiteToSiteClientConfig siteToSiteClientConfig = load(stringWriter.toString());

        List<SiteToSiteRemoteCluster> remoteClusters = siteToSiteClientConfig.getRemoteClusters();
        assertEquals(2, remoteClusters.size());

        SiteToSiteRemoteCluster remote0 = remoteClusters.get(0);
        assertEquals(new HashSet<>(Arrays.asList(remote0Url0, remote0Url1)), remote0.getUrls());

        assertEquals(remote0Keystore, getPrivateString(remote0, "keystoreFilename"));
        assertEquals(remote0KeystorePassword, getPrivateString(remote0, "keystorePassword"));
        assertEquals(remote0KeystoreType, getPrivateString(remote0, "keystoreType"));

        assertEquals(remote0Truststore, getPrivateString(remote0, "truststoreFilename"));
        assertEquals(remote0TruststorePassword, getPrivateString(remote0, "truststorePassword"));
        assertEquals(remote0TruststoreType, getPrivateString(remote0, "truststoreType"));

        assertEquals(remote0ProxyHost, remote0.getProxyHost());
        assertEquals(remote0ProxyPort, remote0.getProxyPort());
        assertEquals(remote0ProxyUsername, remote0.getProxyUsername());
        assertEquals(remote0ProxyPassword, remote0.getProxyPassword());

        assertNull(remote0.getUsername());
        assertNull(remote0.getPassword());

        assertEquals(remote0ClientType, remote0.getClientType());


        SiteToSiteRemoteCluster remote1 = remoteClusters.get(1);
        assertEquals(Collections.singleton(remote1Url0), remote1.getUrls());

        assertNull(getPrivateString(remote1, "keystoreFilename"));
        assertNull(getPrivateString(remote1, "keystorePassword"));
        assertNull(getPrivateString(remote1, "keystoreType"));

        assertEquals(remote1Truststore, getPrivateString(remote1, "truststoreFilename"));
        assertEquals(remote1TruststorePassword, getPrivateString(remote1, "truststorePassword"));
        assertEquals(remote1TruststoreType, getPrivateString(remote1, "truststoreType"));

        assertNull(remote1.getProxyHost());
        assertEquals(0, remote1.getProxyPort());
        assertNull(remote1.getProxyUsername());
        assertNull(remote1.getProxyPassword());

        assertEquals(remote1Username, remote1.getUsername());
        assertEquals(remote1Password, remote1.getPassword());

        assertEquals(SiteToSiteRemoteCluster.ClientType.HTTP, remote1.getClientType());
    }

    @Test
    public void testNoTimeout() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(30, load("").getTimeout(TimeUnit.SECONDS));
    }

    @Test
    public void testTimeoutDefaultUnitMillis() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "timeout=1000").getTimeout(TimeUnit.SECONDS));
    }

    @Test
    public void testTimeoutUnit() throws IOException, SiteToSiteClientConfigCreationException {
        String propertiesText = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "timeout=10\n" + PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "timeout.unit=SECONDS";
        assertEquals(10, load(propertiesText).getTimeout(TimeUnit.SECONDS));
    }

    @Test
    public void testNoIdleConnectionExpiration() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(30, load("").getIdleConnectionExpiration(TimeUnit.SECONDS));
    }

    @Test
    public void testIdleConnectionExpirationDefaultUnitMillis() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "idleConnectionExpiration=1000").getIdleConnectionExpiration(TimeUnit.SECONDS));
    }

    @Test
    public void testIdleConnectionExpirationUnit() throws IOException, SiteToSiteClientConfigCreationException {
        String propertiesText = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "idleConnectionExpiration=10\n" +
                PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "idleConnectionExpiration.unit=SECONDS";
        assertEquals(10, load(propertiesText).getIdleConnectionExpiration(TimeUnit.SECONDS));
    }

    @Test
    public void testUseCompressionDefault() throws IOException, SiteToSiteClientConfigCreationException {
        assertFalse(load("").isUseCompression());
    }

    @Test
    public void testUseCompressionTrue() throws IOException, SiteToSiteClientConfigCreationException {
        assertTrue(load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "useCompression=true").isUseCompression());
    }

    @Test
    public void testUseCompressionFalse() throws IOException, SiteToSiteClientConfigCreationException {
        assertFalse(load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "useCompression=false").isUseCompression());
    }

    @Test
    public void testPortNameDefault() throws IOException, SiteToSiteClientConfigCreationException {
        assertNull(load("").getPortName());
    }

    @Test
    public void testPortName() throws IOException, SiteToSiteClientConfigCreationException {
        String testPortName = "testPortName";
        assertEquals(testPortName, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "portName=" + testPortName).getPortName());
    }

    @Test
    public void testPortIdentifierDefault() throws IOException, SiteToSiteClientConfigCreationException {
        assertNull(load("").getPortIdentifier());
    }

    @Test
    public void testPortIdentifier() throws IOException, SiteToSiteClientConfigCreationException {
        String testPortIdentifier = "testPortIdentifier";
        assertEquals(testPortIdentifier, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "portIdentifier=" + testPortIdentifier).getPortIdentifier());
    }

    @Test
    public void testNoPreferredBatchDuration() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(0, load("").getPreferredBatchDuration(TimeUnit.NANOSECONDS));
    }

    @Test
    public void testPreferredBatchDurationDefaultUnitMillis() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "preferredBatchDuration=1000").getPreferredBatchDuration(TimeUnit.SECONDS));
    }

    @Test
    public void testPreferredBatchDurationUnit() throws IOException, SiteToSiteClientConfigCreationException {
        String propertiesText = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "preferredBatchDuration=10\n" +
                PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "preferredBatchDuration.unit=SECONDS";
        assertEquals(10, load(propertiesText).getPreferredBatchDuration(TimeUnit.SECONDS));
    }

    @Test
    public void testNoPreferredBatchSize() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(0, load("").getPreferredBatchSize());
    }

    @Test
    public void testPreferredBatchSize() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1000, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "preferredBatchSize=1000").getPreferredBatchSize());
    }

    @Test
    public void testNoPreferredBatchCount() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(100, load("").getPreferredBatchCount());
    }

    @Test
    public void testPreferredBatchCount() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1000, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "preferredBatchCount=1000").getPreferredBatchCount());
    }

    @Test
    public void testNoPeerUpdateInterval() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(30, load("").getPeerUpdateInterval(TimeUnit.MINUTES));
    }

    @Test
    public void testPeerUpdateIntervalDefaultUnitMillis() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(100, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "peerUpdateInterval=100000").getPeerUpdateInterval(TimeUnit.SECONDS));
    }

    @Test
    public void testPeerUpdateIntervalUnit() throws IOException, SiteToSiteClientConfigCreationException {
        String propertiesText = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "peerUpdateInterval=1000\n" +
                PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "peerUpdateInterval.unit=SECONDS";
        assertEquals(1000, load(propertiesText).getPeerUpdateInterval(TimeUnit.SECONDS));
    }

    protected void writeLine(BufferedWriter bufferedWriter, String line) throws IOException {
        bufferedWriter.write(line);
        bufferedWriter.newLine();
    }

    protected SiteToSiteClientConfig load(String propertiesText) throws IOException, SiteToSiteClientConfigCreationException {
        Properties properties = new Properties();
        StringReader stringReader = new StringReader(propertiesText);
        try {
            properties.load(stringReader);
        } finally {
            stringReader.close();
        }
        return propertiesSiteToSiteClientConfigFactory.create(properties);
    }

    protected String getPrivateString(Object instance, String fieldName) throws NoSuchFieldException, IllegalAccessException {
        Field declaredField = instance.getClass().getDeclaredField(fieldName);
        declaredField.setAccessible(true);
        return (String) declaredField.get(instance);
    }
}
