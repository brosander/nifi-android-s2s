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

package org.apache.nifi.android.sitetosite.client.http.parser;

import android.util.JsonReader;
import android.util.Log;

import org.apache.nifi.android.sitetosite.client.peer.Peer;
import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Json streaming parser for peer list
 */
public class PeerListParser {
    public static final String CANONICAL_NAME = PeerListParser.class.getCanonicalName();
    public static final String PEERS = "peers";
    public static final String HOSTNAME = "hostname";
    public static final String PORT = "port";
    public static final String SECURE = "secure";
    public static final String FLOW_FILE_COUNT = "flowFileCount";

    public static List<Peer> parsePeers(InputStream inputStream) throws IOException {
        List<Peer> result = null;
        JsonReader jsonReader = new JsonReader(new InputStreamReader(inputStream, Charsets.UTF_8));
        try {
            jsonReader.beginObject();
            try {
                while (jsonReader.hasNext()) {
                    if (PEERS.equals(jsonReader.nextName())) {
                        result = parsePeersArray(jsonReader);
                    } else {
                        jsonReader.skipValue();
                    }
                }
            } finally {
                jsonReader.endObject();
            }
        } finally {
            jsonReader.close();
        }
        return result == null ? Collections.<Peer>emptyList() : result;
    }

    private static List<Peer> parsePeersArray(JsonReader jsonReader) throws IOException {
        List<Peer> result = new ArrayList<>();
        jsonReader.beginArray();
        try {
            while (jsonReader.hasNext()) {
                Peer peer = parsePeer(jsonReader);
                if (peer != null) {
                    result.add(peer);
                }
            }
        } finally {
            jsonReader.endArray();
        }
        return result;
    }

    private static Peer parsePeer(JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        try {
            String hostname = null;
            Integer port = null;
            Boolean secure = null;
            Integer flowFileCount = null;
            while (jsonReader.hasNext()) {
                String name = jsonReader.nextName();
                if (HOSTNAME.equals(name)) {
                    hostname = jsonReader.nextString();
                } else if (PORT.equals(name)) {
                    port = jsonReader.nextInt();
                } else if (SECURE.equals(name)) {
                    secure = jsonReader.nextBoolean();
                } else if (FLOW_FILE_COUNT.equals(name)) {
                    flowFileCount = jsonReader.nextInt();
                } else {
                    jsonReader.skipValue();
                }
            }
            if (hostname == null) {
                Log.w(CANONICAL_NAME, "Null hostname " + peerToString(hostname, port, secure, flowFileCount));
            } else if (port == null) {
                Log.w(CANONICAL_NAME, "Null port " + peerToString(hostname, port, secure, flowFileCount));
            } else if (secure == null) {
                Log.w(CANONICAL_NAME, "Null secure " + peerToString(hostname, port, secure, flowFileCount));
            } else if (flowFileCount == null) {
                Log.w(CANONICAL_NAME, "Null flowFileCount " + peerToString(hostname, port, secure, flowFileCount));
            } else {
                return new Peer(hostname, port, 0, secure, flowFileCount);
            }
        } finally {
            jsonReader.endObject();
        }
        return null;
    }

    private static String peerToString(String hostname, Integer port, Boolean secure, Integer flowFileCount) {
        return "Peer[hostname = " + hostname + ", port = " + port + ", secure = " + secure + ", flowFileCount = " + flowFileCount + "]";
    }
}
