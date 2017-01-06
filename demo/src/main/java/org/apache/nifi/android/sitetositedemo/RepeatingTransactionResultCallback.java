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

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.persistence.TransactionLogEntry;
import org.apache.nifi.android.sitetosite.service.ParcelableTransactionResultCallback;

import java.io.IOException;
import java.util.Date;

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
    public void onSuccess(Context context, TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig) {
        SiteToSiteDB siteToSiteDB = new SiteToSiteDB(context);
        siteToSiteDB.save(new TransactionLogEntry(transactionResult));
        siteToSiteDB.save(siteToSiteClientConfig.getUrls(), siteToSiteClientConfig.getProxyHost(), siteToSiteClientConfig.getProxyPort(), siteToSiteClientConfig.getPeerStatus());
    }

    @Override
    public void onException(Context context, IOException exception, SiteToSiteClientConfig siteToSiteClientConfig) {
        SiteToSiteDB siteToSiteDB = new SiteToSiteDB(context);
        siteToSiteDB.save(new TransactionLogEntry(exception));
        siteToSiteDB.save(siteToSiteClientConfig.getUrls(), siteToSiteClientConfig.getProxyHost(), siteToSiteClientConfig.getProxyPort(), siteToSiteClientConfig.getPeerStatus());
    }
}