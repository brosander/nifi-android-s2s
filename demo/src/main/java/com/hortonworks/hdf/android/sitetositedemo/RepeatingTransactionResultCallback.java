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

package com.hortonworks.hdf.android.sitetositedemo;

import android.content.Context;
import android.os.Parcel;

import com.hortonworks.hdf.android.sitetosite.client.TransactionResult;
import com.hortonworks.hdf.android.sitetosite.service.ParcelableTransactionResultCallback;

import java.io.IOException;

public class RepeatingTransactionResultCallback implements ParcelableTransactionResultCallback {
    public static final Creator<RepeatingTransactionResultCallback> CREATOR = new Creator<RepeatingTransactionResultCallback>() {
        @Override
        public RepeatingTransactionResultCallback createFromParcel(Parcel source) {
            return new RepeatingTransactionResultCallback();
        }

        @Override
        public RepeatingTransactionResultCallback[] newArray(int size) {
            return new RepeatingTransactionResultCallback[1];
        }
    };

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {

    }

    @Override
    public void onSuccess(Context context, TransactionResult transactionResult) {
        new DemoAppDB(context).save(new TransactionLogEntry(transactionResult));
    }

    @Override
    public void onException(Context context, IOException exception) {
        new DemoAppDB(context).save(new TransactionLogEntry(exception));
    }
}