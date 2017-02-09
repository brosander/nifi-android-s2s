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

package org.apache.nifi.android.sitetosite.client.peer;

import android.util.JsonReader;

import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class SiteToSiteInfo {
    public static final String REMOTE_SITE_LISTENING_PORT = "remoteSiteListeningPort";
    public static final String CONTROLLER = "controller";
    public static final String INPUT_PORTS = "inputPorts";
    public static final String ID = "id";
    public static final String NAME = "name";
    private final Integer rawSiteToSitePort;
    private final List<Port> inputPorts;

    public SiteToSiteInfo(InputStream inputStream) throws IOException {
        List<Port> inputPorts = new ArrayList<>();
        Integer rawSiteToSitePort = null;
        JsonReader jsonReader = new JsonReader(new InputStreamReader(inputStream, Charsets.UTF_8));
        try {
            jsonReader.beginObject();
            while (jsonReader.hasNext()) {
                if (CONTROLLER.equals(jsonReader.nextName())) {
                    jsonReader.beginObject();
                    while (jsonReader.hasNext()) {
                        String controllerKey = jsonReader.nextName();
                        if (INPUT_PORTS.equals(controllerKey)) {
                            inputPorts = Port.parsePortArray(jsonReader);
                        } else if (REMOTE_SITE_LISTENING_PORT.equals(controllerKey)) {
                            try {
                                rawSiteToSitePort = jsonReader.nextInt();
                            } catch (Exception e) {
                                rawSiteToSitePort = null;
                            }
                        } else {
                            jsonReader.skipValue();
                        }
                    }
                    jsonReader.endObject();
                } else {
                    jsonReader.skipValue();
                }
            }
            jsonReader.endObject();
        } finally {
            jsonReader.close();
        }
        this.rawSiteToSitePort = rawSiteToSitePort;
        this.inputPorts = inputPorts;
    }

    public String getIdForInputPortName(String name) {
        for (Port inputPort : inputPorts) {
            if (name.equals(inputPort.getName())) {
                return inputPort.getId();
            }
        }
        return null;
    }

    public Integer getRawSiteToSitePort() {
        return rawSiteToSitePort;
    }
}
