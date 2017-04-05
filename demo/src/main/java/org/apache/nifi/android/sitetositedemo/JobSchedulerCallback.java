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

package org.apache.nifi.android.sitetositedemo;

import android.content.Context;
import android.os.Parcel;

import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.service.ParcelableQueuedOperationResultCallback;

import java.io.IOException;

public class JobSchedulerCallback implements ParcelableQueuedOperationResultCallback {
    public static final Creator<JobSchedulerCallback> CREATOR = new Creator<JobSchedulerCallback>() {
        @Override
        public JobSchedulerCallback createFromParcel(Parcel source) {
            return new JobSchedulerCallback();
        }

        @Override
        public JobSchedulerCallback[] newArray(int size) {
            return new JobSchedulerCallback[size];
        }
    };

    @Override
    public void onSuccess(Context context) {
        new DemoAppDB(context).save(new TransactionLogEntry(new TransactionResult(-1, ResponseCode.CONFIRM_TRANSACTION, "JobScheduler processed queued flow file(s) due to criteria match.")));
    }

    @Override
    public void onException(Context context, IOException exception) {
        new DemoAppDB(context).save(new TransactionLogEntry(exception));
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {

    }
}
