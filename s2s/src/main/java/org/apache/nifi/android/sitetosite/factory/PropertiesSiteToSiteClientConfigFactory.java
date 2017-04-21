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
import org.apache.nifi.android.sitetosite.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PropertiesSiteToSiteClientConfigFactory implements SiteToSiteClientConfigFactory<Properties> {
    public static final String S2S_CONFIG = "s2s.config.";
    public static final String S2S_CONFIG_REMOTE = "s2s.config.remote.cluster.";

    @Override
    public SiteToSiteClientConfig create(Properties input) throws SiteToSiteClientConfigCreationException {
        SiteToSiteClientConfig result = new SiteToSiteClientConfig();
        List<SiteToSiteRemoteCluster> siteToSiteRemoteClusters = new ArrayList<>();
        for (int index = 0;; index++) {
            SiteToSiteRemoteCluster remoteCluster = createRemoteCluster(index, input);
            if (remoteCluster == null) {
                break;
            }
            siteToSiteRemoteClusters.add(remoteCluster);
        }
        result.setRemoteClusters(siteToSiteRemoteClusters);

        Long durationNanos = getDurationNanos(input, S2S_CONFIG + "timeout");
        if (durationNanos != null) {
            result.setTimeout(durationNanos, TimeUnit.NANOSECONDS);
        }

        Long idleConnectionExpirationNanos = getDurationNanos(input, S2S_CONFIG + "idleConnectionExpiration");
        if (idleConnectionExpirationNanos != null) {
            result.setIdleConnectionExpiration(idleConnectionExpirationNanos, TimeUnit.NANOSECONDS);
        }

        result.setUseCompression(Boolean.valueOf(input.getProperty(S2S_CONFIG + "useCompression", "false")));
        result.setPortName(getPropEmptyToNull(input, S2S_CONFIG + "portName"));
        result.setPortIdentifier(getPropEmptyToNull(input, S2S_CONFIG + "portIdentifier"));

        Long preferredBatchDuration = getDurationNanos(input, S2S_CONFIG + "preferredBatchDuration");
        if (preferredBatchDuration != null) {
            result.setPreferredBatchDuration(preferredBatchDuration, TimeUnit.NANOSECONDS);
        }

        String preferredBatchSize = getPropEmptyToNull(input, S2S_CONFIG + "preferredBatchSize");
        if (preferredBatchSize != null) {
            result.setPreferredBatchSize(Long.parseLong(preferredBatchSize));
        }

        String preferredBatchCount = getPropEmptyToNull(input, S2S_CONFIG + "preferredBatchCount");
        if (preferredBatchCount != null) {
            result.setPreferredBatchCount(Integer.parseInt(preferredBatchCount));
        }

        Long peerUpdateInterval = getDurationNanos(input, S2S_CONFIG + "peerUpdateInterval");
        if (peerUpdateInterval != null) {
            result.setPeerUpdateInterval(peerUpdateInterval, TimeUnit.NANOSECONDS);
        }

        return result;
    }

    protected SiteToSiteRemoteCluster createRemoteCluster(int index, Properties properties) {
        String propBase = S2S_CONFIG_REMOTE + index + ".";
        SiteToSiteRemoteCluster siteToSiteRemoteCluster = new SiteToSiteRemoteCluster();

        List<String> urls = new ArrayList<>();
        for (int urlIndex = 0;; urlIndex++){
            String url = properties.getProperty(propBase + "url." + urlIndex);
            if (StringUtils.isNullOrEmpty(url)) {
                break;
            }
            urls.add(url);
        }
        if (urls.size() == 0) {
            return null;
        }
        siteToSiteRemoteCluster.setUrls(urls);
        siteToSiteRemoteCluster.setKeystoreFilename(getPropEmptyToNull(properties, propBase + "keystore"));
        siteToSiteRemoteCluster.setKeystorePassword(getPropEmptyToNull(properties, propBase + "keystorePasswd"));
        siteToSiteRemoteCluster.setKeystoreType(getPropEmptyToNull(properties, propBase + "keystoreType"));

        siteToSiteRemoteCluster.setTruststoreFilename(getPropEmptyToNull(properties, propBase + "truststore"));
        siteToSiteRemoteCluster.setTruststorePassword(getPropEmptyToNull(properties, propBase + "truststorePasswd"));
        siteToSiteRemoteCluster.setTruststoreType(getPropEmptyToNull(properties, propBase + "truststoreType"));

        siteToSiteRemoteCluster.setProxyHost(getPropEmptyToNull(properties, propBase + "proxyHost"));
        String portString = getPropEmptyToNull(properties, propBase + "proxyPort");
        if (portString != null) {
            siteToSiteRemoteCluster.setProxyPort(Integer.parseInt(portString));
        }
        String proxyAuthorizationType = getPropEmptyToNull(properties, propBase + "proxyAuthorizationType");
        if (proxyAuthorizationType != null) {
            siteToSiteRemoteCluster.setProxyAuthorizationType(proxyAuthorizationType);
        }
        siteToSiteRemoteCluster.setProxyUsername(getPropEmptyToNull(properties, propBase + "proxyUsername"));
        siteToSiteRemoteCluster.setProxyPassword(getPropEmptyToNull(properties, propBase + "proxyPassword"));

        siteToSiteRemoteCluster.setUsername(getPropEmptyToNull(properties, propBase + "username"));
        siteToSiteRemoteCluster.setPassword(getPropEmptyToNull(properties, propBase + "password"));

        String clientType = getPropEmptyToNull(properties, propBase + "clientType");
        if (clientType != null) {
            siteToSiteRemoteCluster.setClientType(SiteToSiteRemoteCluster.ClientType.valueOf(clientType));
        }

        return siteToSiteRemoteCluster;
    }

    protected String getPropEmptyToNull(Properties properties, String key) {
        return StringUtils.emptyToNull(properties.getProperty(key));
    }

    protected Long getDurationNanos(Properties properties, String propBase) {
        String durationString = StringUtils.emptyToNull(properties.getProperty(propBase));
        if (durationString == null) {
            return null;
        }
        long duration = Long.parseLong(durationString);
        String unitString = StringUtils.emptyToNull(properties.getProperty(propBase + ".unit"));
        TimeUnit timeUnit = TimeUnit.MILLISECONDS;
        if (unitString != null) {
            timeUnit = TimeUnit.valueOf(unitString);
        }
        return timeUnit.toNanos(duration);
    }
}
