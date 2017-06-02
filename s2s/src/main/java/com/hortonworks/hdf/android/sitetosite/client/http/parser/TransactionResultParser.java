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

package com.hortonworks.hdf.android.sitetosite.client.http.parser;

import android.util.JsonReader;

import com.hortonworks.hdf.android.sitetosite.client.TransactionResult;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.util.Charsets;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Json streaming parser for getting transaction result
 */
public class TransactionResultParser {
    public static final String FLOW_FILE_SENT = "flowFileSent";
    public static final String RESPONSE_CODE = "responseCode";
    public static final String MESSAGE = "message";

    public static TransactionResult parseTransactionResult(InputStream inputStream) throws IOException {
        int flowFilesSent = 0;
        int responseCode = -1;
        String message = null;
        JsonReader jsonReader = new JsonReader(new InputStreamReader(inputStream, Charsets.UTF_8));
        try {
            jsonReader.beginObject();
            try {
                while (jsonReader.hasNext()) {
                    String nextName = jsonReader.nextName();
                    if (FLOW_FILE_SENT.equals(nextName)) {
                        flowFilesSent = jsonReader.nextInt();
                    } else if (RESPONSE_CODE.equals(nextName)) {
                        responseCode = jsonReader.nextInt();
                    } else if (MESSAGE.equals(nextName)) {
                        message = jsonReader.nextString();
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
        return new TransactionResult(flowFilesSent, responseCode == -1 ? null : ResponseCode.fromCode(responseCode), message);
    }
}
