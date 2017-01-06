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

package org.apache.nifi.android.sitetosite.client.parser;

import android.util.JsonReader;

import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class PortIdentifierParser {
    public static String getPortIdentifier(InputStream inputStream, String portName) throws IOException {
        JsonReader jsonReader = new JsonReader(new InputStreamReader(inputStream, Charsets.UTF_8));
        try {
            return getPortIdentifierFromController(portName, jsonReader);
        } finally {
            jsonReader.close();
        }
    }

    private static String getPortIdentifierFromController(String portName, JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        String id = null;
        while (jsonReader.hasNext()) {
            if (id == null && "controller".equals(jsonReader.nextName())) {
                id = getPortIdentifierFromInputPorts(portName, jsonReader);
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        return id;
    }

    private static String getPortIdentifierFromInputPorts(String portName, JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        String id = null;
        while (jsonReader.hasNext()) {
            if ("inputPorts".equals(jsonReader.nextName())) {
                jsonReader.beginArray();
                while (jsonReader.hasNext()) {
                    if (id == null) {
                        id = getPortIdentifierFromInputPort(portName, jsonReader);
                    } else {
                        jsonReader.skipValue();
                    }
                }
                jsonReader.endArray();
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        return id;
    }

    private static String getPortIdentifierFromInputPort(String portName, JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        String id = null;
        String name = null;
        while (jsonReader.hasNext()) {
            String key = jsonReader.nextName();
            if (id == null && "id".equals(key)) {
                id = jsonReader.nextString();
            } else if (name == null && "name".equals(key)) {
                name = jsonReader.nextString();
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        if (portName.equals(name)) {
            return id;
        }
        return null;
    }
}
