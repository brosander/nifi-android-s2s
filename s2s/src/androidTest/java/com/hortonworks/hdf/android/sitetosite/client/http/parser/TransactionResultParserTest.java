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

import com.hortonworks.hdf.android.sitetosite.client.TransactionResult;
import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;
import com.hortonworks.hdf.android.sitetosite.util.Charsets;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class TransactionResultParserTest {
    @Test
    public void testNull() throws IOException, JSONException {
        TransactionResult parse = parse(null, null, null);
        assertEquals(0, parse.getFlowFilesSent());
        assertNull(parse.getResponseCode());
        assertNull(parse.getMessage());
    }

    @Test
    public void testFlowFilesSent() throws IOException, JSONException {
        assertEquals(123, parse(123, null, null).getFlowFilesSent());
    }

    @Test
    public void testResponseCode() throws IOException, JSONException {
        assertEquals(ResponseCode.TRANSACTION_FINISHED, parse(null, ResponseCode.TRANSACTION_FINISHED, null).getResponseCode());
    }

    @Test
    public void testMessage() throws IOException, JSONException {
        String message = "test message";
        assertEquals(message, parse(null, null, message).getMessage());
    }

    private TransactionResult parse(Integer flowFilesTransferred, ResponseCode responseCode, String message) throws JSONException, IOException {
        JSONObject jsonObject = new JSONObject();
        if (flowFilesTransferred != null) {
            jsonObject.put(TransactionResultParser.FLOW_FILE_SENT, flowFilesTransferred);
        }
        if (responseCode == null) {
            jsonObject.put(TransactionResultParser.RESPONSE_CODE, -1);
        } else {
            jsonObject.put(TransactionResultParser.RESPONSE_CODE, responseCode.getCode());
        }
        if (message != null) {
            jsonObject.put(TransactionResultParser.MESSAGE, message);
        }
        return TransactionResultParser.parseTransactionResult(new ByteArrayInputStream(jsonObject.toString().getBytes(Charsets.UTF_8)));
    }
}
