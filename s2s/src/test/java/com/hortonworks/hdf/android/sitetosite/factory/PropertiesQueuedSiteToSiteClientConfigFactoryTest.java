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
import com.hortonworks.hdf.android.sitetosite.client.queued.NoOpDataPacketPrioritizer;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertiesQueuedSiteToSiteClientConfigFactoryTest extends PropertiesSiteToSiteClientConfigFactoryTest {
    @Override
    protected PropertiesSiteToSiteClientConfigFactory initFactory() {
        return new PropertiesQueuedSiteToSiteClientConfigFactory();
    }

    @Override
    protected QueuedSiteToSiteClientConfig load(String propertiesText) throws IOException, SiteToSiteClientConfigCreationException {
        return (QueuedSiteToSiteClientConfig) super.load(propertiesText);
    }

    @Test
    public void testNoMaxRows() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(10000, load("").getMaxRows());
    }

    @Test
    public void testMaxRows() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(1000, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "maxRows=1000").getMaxRows());
    }

    @Test
    public void testNoMaxSize() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(10240000, load("").getMaxSize());
    }

    @Test
    public void testMaxSize() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(100000000, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "maxRows=100000000").getMaxRows());
    }

    @Test
    public void testNoMaxTransactionTime() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(10, load("").getMaxTransactionTime(TimeUnit.MINUTES));
    }

    @Test
    public void testMaxTransactionTimeDefaultUnitMillis() throws IOException, SiteToSiteClientConfigCreationException {
        assertEquals(100, load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "maxTransactionTime=100000").getMaxTransactionTime(TimeUnit.SECONDS));
    }

    @Test
    public void testMaxTransactionTimeUnit() throws IOException, SiteToSiteClientConfigCreationException {
        String propertiesText = PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "maxTransactionTime=1000\n" + PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "maxTransactionTime.unit=SECONDS";
        assertEquals(1000, load(propertiesText).getMaxTransactionTime(TimeUnit.SECONDS));
    }

    @Test
    public void testNoDataPacketPrioritizer() throws IOException, SiteToSiteClientConfigCreationException {
        assertTrue(load("").getDataPacketPrioritizer() instanceof NoOpDataPacketPrioritizer);
    }

    @Test
    public void testDataPacketPrioritizer() throws IOException, SiteToSiteClientConfigCreationException {
        assertTrue(load(PropertiesSiteToSiteClientConfigFactory.S2S_CONFIG + "dataPacketPrioritizerClass=" + TestDataPacketPrioritizer.class.getCanonicalName()).getDataPacketPrioritizer() instanceof TestDataPacketPrioritizer);
    }
}
