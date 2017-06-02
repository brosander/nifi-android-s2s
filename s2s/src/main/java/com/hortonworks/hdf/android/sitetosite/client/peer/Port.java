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

package com.hortonworks.hdf.android.sitetosite.client.peer;

import android.util.JsonReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hortonworks.hdf.android.sitetosite.client.peer.SiteToSiteInfo.ID;
import static com.hortonworks.hdf.android.sitetosite.client.peer.SiteToSiteInfo.NAME;

public class Port {
    private final String id;
    private final String name;

    public Port(JsonReader jsonReader) throws IOException {
        jsonReader.beginObject();
        String id = null;
        String name = null;
        while (jsonReader.hasNext()) {
            String key = jsonReader.nextName();
            if (id == null && ID.equals(key)) {
                id = jsonReader.nextString();
            } else if (name == null && NAME.equals(key)) {
                name = jsonReader.nextString();
            } else {
                jsonReader.skipValue();
            }
        }
        jsonReader.endObject();
        this.id = id;
        this.name = name;
    }

    public static List<Port> parsePortArray(JsonReader jsonReader) throws IOException {
        List<Port> inputPorts = new ArrayList<>();
        jsonReader.beginArray();
        while (jsonReader.hasNext()) {
            inputPorts.add(new Port(jsonReader));
        }
        jsonReader.endArray();
        return inputPorts;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }
}
