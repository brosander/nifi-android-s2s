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
import android.os.SystemClock;

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PeerInstrumentedTest {
    private String testUrl;
    private int testFlowFileCount;
    private long testLastFailure;
    private Peer peer;

    @Before
    public void setup() {
        testUrl = "https://test.url:9443/nifi-api";
        testFlowFileCount = 22;
        testLastFailure = SystemClock.elapsedRealtime() - 10;
        peer = new Peer(testUrl, testFlowFileCount, testLastFailure);
    }

    @Test
    public void testTwoArgConstructor() {
        Peer peer = new Peer(testUrl, testFlowFileCount);
        assertEquals(testUrl, peer.getUrl());
        assertEquals(testFlowFileCount, peer.getFlowFileCount());
        assertEquals(0L, peer.getLastFailure());
    }

    @Test
    public void testThreeArgConstructor() {
        assertEquals(testUrl, peer.getUrl());
        assertEquals(testFlowFileCount, peer.getFlowFileCount());
        assertEquals(testLastFailure, peer.getLastFailure());
    }

    @Test
    public void testParcelable() {
        Parcel parcel = Parcel.obtain();
        peer.writeToParcel(parcel, 0);
        parcel.setDataPosition(0);

        Peer fromParcel = Peer.CREATOR.createFromParcel(parcel);
        assertEquals(testUrl, fromParcel.getUrl());
        assertEquals(testFlowFileCount, fromParcel.getFlowFileCount());
        assertEquals(testLastFailure, fromParcel.getLastFailure());
    }
}
