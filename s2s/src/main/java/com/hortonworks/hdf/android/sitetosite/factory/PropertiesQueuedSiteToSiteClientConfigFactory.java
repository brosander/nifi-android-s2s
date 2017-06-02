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

package com.hortonworks.hdf.android.sitetosite.factory;

import com.hortonworks.hdf.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import com.hortonworks.hdf.android.sitetosite.client.queued.DataPacketPrioritizer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PropertiesQueuedSiteToSiteClientConfigFactory extends PropertiesSiteToSiteClientConfigFactory implements QueuedSiteToSiteClientConfigFactory<Properties> {
    @Override
    public QueuedSiteToSiteClientConfig create(Properties input) throws SiteToSiteClientConfigCreationException {
        QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig = new QueuedSiteToSiteClientConfig(super.create(input));

        String maxRows = getPropEmptyToNull(input, S2S_CONFIG + "maxRows");
        if (maxRows != null) {
            queuedSiteToSiteClientConfig.setMaxRows(Long.parseLong(maxRows));
        }

        String maxSize = getPropEmptyToNull(input, S2S_CONFIG + "maxSize");
        if (maxSize != null) {
            queuedSiteToSiteClientConfig.setMaxSize(Long.parseLong(maxSize));
        }

        Long maxTransactionTime = getDurationNanos(input, S2S_CONFIG + "maxTransactionTime");
        if (maxTransactionTime != null) {
            queuedSiteToSiteClientConfig.setMaxTransactionTime(maxTransactionTime, TimeUnit.NANOSECONDS);
        }

        String dataPacketPrioritizerClass = getPropEmptyToNull(input, S2S_CONFIG + "dataPacketPrioritizerClass");
        if (dataPacketPrioritizerClass != null) {
            Object instance;
            try {
                instance = Class.forName(dataPacketPrioritizerClass).newInstance();
            } catch (Exception e) {
                throw new SiteToSiteClientConfigCreationException("Unable to create instance of dataPacketPrioritizerClass " + dataPacketPrioritizerClass, e);
            }
            if (!(instance instanceof DataPacketPrioritizer)) {
                throw new SiteToSiteClientConfigCreationException("dataPacketPrioritizerClass " + dataPacketPrioritizerClass + " cannot be cast to " + DataPacketPrioritizer.class);
            }
            queuedSiteToSiteClientConfig.setDataPacketPrioritizer((DataPacketPrioritizer) instance);
        }

        return queuedSiteToSiteClientConfig;
    }
}
