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
import android.os.Parcelable;
import android.support.v4.content.WakefulBroadcastReceiver;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.collectors.DataCollector;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.service.SiteToSiteService.IntentType;
import org.apache.nifi.android.sitetosite.util.SerializationUtils;

import java.io.IOException;
import java.util.Random;

public class SiteToSiteRepeating extends WakefulBroadcastReceiver {
    public static final String DATA_COLLECTOR = "DATA_COLLECTOR";
    public static final String REQUEST_CODE = "REQUEST_CODE";
    public static final String REPEATING_INTENT = "REPEATING_INTENT";
    private static final Random random = new Random();

    @Override
    public void onReceive(final Context context, Intent intent) {
        DataCollector dataCollector = SerializationUtils.getParcelable(intent, DATA_COLLECTOR);
        SiteToSiteClientConfig siteToSiteClientConfig = SerializationUtils.getParcelable(intent, SiteToSiteService.SITE_TO_SITE_CONFIG);

        IntentType intentType = getIntentType(intent);
        Parcelable callback = SerializationUtils.getParcelable(intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        int requestCode = getRequestCode(intent);
        Intent repeatingIntent = getIntent(context, intentType, dataCollector, siteToSiteClientConfig, requestCode, callback);

        Iterable<DataPacket> dataPackets = null;
        // Update the pending intent with any state change in data collector
        if (dataCollector != null) {
            dataPackets = dataCollector.getDataPackets();
            PendingIntent.getBroadcast(context, requestCode, repeatingIntent, PendingIntent.FLAG_UPDATE_CURRENT);
        }

        final Intent packetIntent;
        if (intentType == IntentType.SEND) {
            final ParcelableTransactionResultCallback transactionResultCallback = (ParcelableTransactionResultCallback) callback;
            packetIntent = SiteToSiteService.getIntent(context, intentType, dataPackets, siteToSiteClientConfig, new TransactionResultCallback() {
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
        } else {
            final ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback = (ParcelableQueuedOperationResultCallback) callback;
            packetIntent = SiteToSiteService.getIntent(context, intentType, dataPackets, (QueuedSiteToSiteClientConfig) siteToSiteClientConfig, new QueuedOperationResultCallback() {
                @Override
                public Handler getHandler() {
                    return null;
                }

                @Override
                public void onSuccess(QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig) {
                    parcelableQueuedOperationResultCallback.onSuccess(context, queuedSiteToSiteClientConfig);
                }

                @Override
                public void onException(IOException exception, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig) {
                    parcelableQueuedOperationResultCallback.onException(context, exception, queuedSiteToSiteClientConfig);
                }
            }, true);
        }
        SerializationUtils.putParcelable(repeatingIntent, packetIntent, REPEATING_INTENT);
        startWakefulService(context, packetIntent);
    }

    /**
     * Creates a pending intent suitable for use with AlarmManager to schedule repeating site-to-site operations
     *
     * @param context the context
     * @param dataCollector a data collector
     * @param siteToSiteClientConfig the site to site config
     * @param parcelableTransactionResultCallback a callback to be invoked whenever a transaction completes
     * @return a repeatable intent with enough metadata to save and reload it if desired
     */
    public static SiteToSiteRepeatableIntent createSendPendingIntent(Context context, DataCollector dataCollector, SiteToSiteClientConfig siteToSiteClientConfig, ParcelableTransactionResultCallback parcelableTransactionResultCallback) {
        return createPendingIntent(context, IntentType.SEND, dataCollector, siteToSiteClientConfig, parcelableTransactionResultCallback);
    }

    public static SiteToSiteRepeatableIntent createEnqueuePendingIntent(Context context, DataCollector dataCollector, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback) {
        return createPendingIntent(context, IntentType.ENQUEUE, dataCollector, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
    }

    public static SiteToSiteRepeatableIntent createProcessQueuePendingIntent(Context context, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback) {
        return createPendingIntent(context, IntentType.PROCESS, null, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
    }

    public static SiteToSiteRepeatableIntent createCleanupQueuePendingIntent(Context context, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, ParcelableQueuedOperationResultCallback parcelableQueuedOperationResultCallback) {
        return createPendingIntent(context, IntentType.CLEANUP, null, queuedSiteToSiteClientConfig, parcelableQueuedOperationResultCallback);
    }

    synchronized static SiteToSiteRepeatableIntent createPendingIntent(Context context, IntentType intentType, DataCollector dataCollector, SiteToSiteClientConfig siteToSiteClientConfig, Parcelable parcelableTransactionResultCallback) {
        Intent intent = getIntent(context, intentType, dataCollector, siteToSiteClientConfig, null, parcelableTransactionResultCallback);
        int requestCode = getRequestCode(intent);
        return new SiteToSiteRepeatableIntent(requestCode, intent, PendingIntent.getBroadcast(context, requestCode, intent, PendingIntent.FLAG_UPDATE_CURRENT));
    }

    static void updateIntentConfig(Context context, Intent intent, SiteToSiteClientConfig siteToSiteClientConfig) {
        int requestCode = getRequestCode(intent);
        DataCollector dataCollector = SerializationUtils.getParcelable(intent, DATA_COLLECTOR);
        ParcelableTransactionResultCallback transactionResultCallback = SerializationUtils.getParcelable(intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        PendingIntent.getBroadcast(context, requestCode, getIntent(context, getIntentType(intent), dataCollector, siteToSiteClientConfig, requestCode, transactionResultCallback), PendingIntent.FLAG_UPDATE_CURRENT);
    }

    private static Intent getIntent(Context context, IntentType intentType, DataCollector dataCollector, SiteToSiteClientConfig siteToSiteClientConfig, Integer requestCode, Parcelable transactionResultCallback) {
        Intent intent = new Intent(context, SiteToSiteRepeating.class);
        intent.setExtrasClassLoader(dataCollector.getClass().getClassLoader());
        intent.putExtra(SiteToSiteService.INTENT_TYPE, intentType.name());

        if (requestCode == null) {
            // Find unused requestCode
            PendingIntent existing;
            do {
                requestCode = random.nextInt();
                existing = PendingIntent.getBroadcast(context, requestCode, intent, PendingIntent.FLAG_NO_CREATE);
            } while (existing != null);
        }

        intent.putExtra(REQUEST_CODE, requestCode);
        SerializationUtils.putParcelable(dataCollector, intent, DATA_COLLECTOR);
        SerializationUtils.putParcelable(transactionResultCallback, intent, SiteToSiteService.TRANSACTION_RESULT_CALLBACK);
        SerializationUtils.putParcelable(siteToSiteClientConfig, intent, SiteToSiteService.SITE_TO_SITE_CONFIG);
        return intent;
    }

    private static int getRequestCode(Intent intent) {
        return intent.getExtras().getInt(REQUEST_CODE);
    }

    private static IntentType getIntentType(Intent intent) {
        return IntentType.valueOf(intent.getStringExtra(SiteToSiteService.INTENT_TYPE));
    }
}
