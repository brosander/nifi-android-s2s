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

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PeerTest {
    @Test
    public void testComparable() {
        Peer peer1 = new Peer("a", 0, 0);
        Peer peer2 = new Peer("b", 1, 0);
        Peer peer3 = new Peer("c", 2, 0);
        Peer peer4 = new Peer("d", 0, 1);
        Peer peer5 = new Peer("e", 0, 2);
        Peer peer6 = new Peer("f", 0, 2);
        List<Peer> peers = new ArrayList<>(Arrays.asList(peer1, peer2, peer3, peer4, peer5, peer6));
        Collections.sort(peers);
        assertEquals(peer1, peers.get(0));
        assertEquals(peer2, peers.get(1));
        assertEquals(peer3, peers.get(2));
        assertEquals(peer4, peers.get(3));
        assertEquals(peer5, peers.get(4));
        assertEquals(peer6, peers.get(5));
    }
}
