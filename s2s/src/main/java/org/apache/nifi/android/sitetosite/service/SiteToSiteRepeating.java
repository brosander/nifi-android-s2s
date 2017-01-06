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

package org.apache.nifi.android.sitetosite.service;

import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.os.Handler;
import android.support.v4.content.WakefulBroadcastReceiver;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.collectors.DataCollector;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.IntentUtils;

import java.io.IOException;
import java.util.Random;

public class SiteToSiteRepeating extends WakefulBroadcastReceiver {
    public static final String DATA_COLLECTOR = "DATA_COLLECTOR";
    public static final String REQUEST_CODE = "REQUEST_CODE";
    public static final String REPEATING_INTENT = "REPEATING_INTENT";
    private static final Random random = new Random();

    @Override
    public void onReceive(final Context context, Intent intent) {
        DataCollector dataCollector = IntentUtils.getParcelable(intent, DATA_COLLECTOR);
        SiteToSiteClientConfig siteToSiteClientConfig = IntentUtils.getParcelable(intent, SiteToSiteService.SITE_TO_SITE_CONFIG);
        Iterable<DataPacket> dataPackets = dataCollector.getDataPackets();

        // Update the pending intent with any state change in data collector
        int requestCode = getRequestCode(intent);
        final ParcelableTransactionResultCallback transactionResultCallback = IntentUtils.getParcelable(intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        Intent repeatingIntent = getIntent(context, dataCollector, siteToSiteClientConfig, requestCode, transactionResultCallback);
        PendingIntent.getBroadcast(context, requestCode, repeatingIntent, PendingIntent.FLAG_UPDATE_CURRENT);

        Intent packetIntent = SiteToSiteService.getIntent(context, dataPackets, siteToSiteClientConfig, new TransactionResultCallback() {
            @Override
            public Handler getHandler() {
                return null;
            }

            @Override
            public void onSuccess(TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig) {
                transactionResultCallback.onSuccess(context, transactionResult, siteToSiteClientConfig);
            }

            @Override
            public void onException(IOException exception, SiteToSiteClientConfig siteToSiteClientConfig) {
                transactionResultCallback.onException(context, exception, siteToSiteClientConfig);
            }
        }, true);
        IntentUtils.putParcelable(repeatingIntent, packetIntent, REPEATING_INTENT);
        startWakefulService(context, packetIntent);
    }

    public synchronized static SiteToSiteRepeatableIntent createPendingIntent(Context context, DataCollector dataCollector, SiteToSiteClientConfig siteToSiteClientConfig, ParcelableTransactionResultCallback transactionResultCallback) {
        Intent intent = getIntent(context, dataCollector, siteToSiteClientConfig, null, transactionResultCallback);
        int requestCode = getRequestCode(intent);
        return new SiteToSiteRepeatableIntent(requestCode, intent, PendingIntent.getBroadcast(context, requestCode, intent, PendingIntent.FLAG_UPDATE_CURRENT));
    }

    static void updateIntentConfig(Context context, Intent intent, SiteToSiteClientConfig siteToSiteClientConfig) {
        int requestCode = getRequestCode(intent);
        DataCollector dataCollector = IntentUtils.getParcelable(intent, DATA_COLLECTOR);
        ParcelableTransactionResultCallback transactionResultCallback = IntentUtils.getParcelable(intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        PendingIntent.getBroadcast(context, requestCode, getIntent(context, dataCollector, siteToSiteClientConfig, requestCode, transactionResultCallback), PendingIntent.FLAG_UPDATE_CURRENT);
    }

    private static Intent getIntent(Context context, DataCollector dataCollector, SiteToSiteClientConfig siteToSiteClientConfig, Integer requestCode, ParcelableTransactionResultCallback transactionResultCallback) {
        Intent intent = new Intent(context, SiteToSiteRepeating.class);
        intent.setExtrasClassLoader(dataCollector.getClass().getClassLoader());

        if (requestCode == null) {
            // Find unused requestCode
            PendingIntent existing;
            do {
                requestCode = random.nextInt();
                existing = PendingIntent.getBroadcast(context, requestCode, intent, PendingIntent.FLAG_NO_CREATE);
            } while (existing != null);
        }

        intent.putExtra(REQUEST_CODE, requestCode);
        IntentUtils.putParcelable(dataCollector, intent, DATA_COLLECTOR);
        IntentUtils.putParcelable(transactionResultCallback, intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        IntentUtils.putParcelable(siteToSiteClientConfig, intent, SiteToSiteService.SITE_TO_SITE_CONFIG);
        return intent;
    }

    private static int getRequestCode(Intent intent) {
        return intent.getExtras().getInt(REQUEST_CODE);
    }
}
