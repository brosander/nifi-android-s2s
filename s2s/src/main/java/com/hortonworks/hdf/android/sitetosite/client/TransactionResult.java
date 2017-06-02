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

package com.hortonworks.hdf.android.sitetosite.client;

import android.os.Parcel;
import android.os.Parcelable;

import com.hortonworks.hdf.android.sitetosite.client.protocol.ResponseCode;

/**
 * Result of a transaction
 */
public class TransactionResult implements Parcelable {
    private final int flowFilesSent;
    private final ResponseCode responseCode;
    private final String message;

    public static final Creator<TransactionResult> CREATOR = new Creator<TransactionResult>() {
        @Override
        public TransactionResult createFromParcel(Parcel source) {
            int flowFilesSent = source.readInt();
            ResponseCode responseCode = ResponseCode.fromCode(source.readInt());
            String message = source.readString();
            return new TransactionResult(flowFilesSent, responseCode, message);
        }

        @Override
        public TransactionResult[] newArray(int size) {
            return new TransactionResult[size];
        }
    };

    public TransactionResult(int flowFilesSent, ResponseCode responseCode, String message) {
        this.flowFilesSent = flowFilesSent;
        this.responseCode = responseCode;
        this.message = message;
    }

    /**
     * Gets the number of flowFiles sent
     *
     * @return the number of flowFiles sent
     */
    public int getFlowFilesSent() {
        return flowFilesSent;
    }

    /**
     * Gets the response code
     *
     * @return the response code
     */
    public ResponseCode getResponseCode() {
        return responseCode;
    }

    /**
     * Gets the message
     *
     * @return the message
     */
    public String getMessage() {
        return message;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(flowFilesSent);
        dest.writeInt(responseCode == null ? -1 : responseCode.getCode());
        dest.writeString(message);
    }
}
