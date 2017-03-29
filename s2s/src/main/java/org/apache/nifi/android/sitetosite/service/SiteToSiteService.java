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

import android.app.IntentService;
import android.content.Context;
import android.content.Intent;
import android.os.PowerManager;
import android.os.ResultReceiver;
import android.support.v4.content.WakefulBroadcastReceiver;
import android.util.Log;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.SerializationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SiteToSiteService extends IntentService {
    public static final String IO_EXCEPTION = "IOException";
    public static final String RESULT = "RESULT";
    public static final String INTENT_TYPE = "INTENT_TYPE";
    public static final String DATA_PACKETS = "DATA_PACKETS";
    public static final String TRANSACTION_RESULT_CALLBACK = "TRANSACTION_RESULT_CALLBACK";
    public static final String SHOULD_COMPLETE_WAKEFUL_INTENT = "SHOULD_COMPLETE_WAKEFUL_INTENT";
    public static final String SITE_TO_SITE_CONFIG = "SITE_TO_SITE_CONFIG";
    public static final String CANONICAL_NAME = SiteToSiteService.class.getCanonicalName();

    public SiteToSiteService() {
        super(SiteToSiteService.class.getName());
    }

    public static void sendDataPackets(Context context, Iterable<DataPacket> packets, SiteToSiteClientConfig siteToSiteClientConfig, TransactionResultCallback transactionResultCallback) {
        context.startService(getIntent(context, IntentType.SEND, packets, siteToSiteClientConfig, transactionResultCallback));
    }

    public static void sendDataPacket(Context context, DataPacket packet, SiteToSiteClientConfig siteToSiteClientConfig, TransactionResultCallback transactionResultCallback) {
        context.startService(getIntent(context, IntentType.SEND, Collections.singletonList(packet), siteToSiteClientConfig, transactionResultCallback));
    }

    public static void enqueueDataPackets(Context context, Iterable<DataPacket> packets, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback) {
        context.startService(getIntent(context, IntentType.ENQUEUE, packets, queuedSiteToSiteClientConfig, queuedOperationResultCallback));
    }

    public static void enqueueDataPacket(Context context, DataPacket packet, QueuedSiteToSiteClientConfig siteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback) {
        context.startService(getIntent(context, IntentType.ENQUEUE, Collections.singletonList(packet), siteToSiteClientConfig, queuedOperationResultCallback));
    }

    public static void processQueuedPackets(Context context, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback) {
        context.startService(getIntent(context, IntentType.PROCESS, null, queuedSiteToSiteClientConfig, queuedOperationResultCallback));
    }

    public static void cleanupQueuedPackets(Context context, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback) {
        context.startService(getIntent(context, IntentType.CLEANUP, null, queuedSiteToSiteClientConfig, queuedOperationResultCallback));
    }

    public static Intent getIntent(Context context, IntentType intentType, Iterable<DataPacket> packets, SiteToSiteClientConfig siteToSiteClientConfig, TransactionResultCallback transactionResultCallback) {
        return getIntent(context, intentType, packets, siteToSiteClientConfig, transactionResultCallback, false);
    }

    public static Intent getIntent(Context context, IntentType intentType, Iterable<DataPacket> packets, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback) {
        return getIntent(context, intentType, packets, queuedSiteToSiteClientConfig, queuedOperationResultCallback, false);
    }

    public static Intent getIntent(Context context, IntentType intentType, Iterable<DataPacket> packets, SiteToSiteClientConfig siteToSiteClientConfig, TransactionResultCallback transactionResultCallback, boolean completeWakefulIntent) {
        Intent intent = getIntent(context, intentType, packets, completeWakefulIntent);
        if (transactionResultCallback != null) {
            intent.putExtra(TRANSACTION_RESULT_CALLBACK, TransactionResultCallback.Receiver.wrap(transactionResultCallback));
        }
        SerializationUtils.putParcelable(siteToSiteClientConfig, intent, SITE_TO_SITE_CONFIG);
        return intent;
    }

    public static Intent getIntent(Context context, IntentType intentType, Iterable<DataPacket> packets, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig, QueuedOperationResultCallback queuedOperationResultCallback, boolean completeWakefulIntent) {
        Intent intent = getIntent(context, intentType, packets, completeWakefulIntent);
        if (queuedOperationResultCallback != null) {
            intent.putExtra(TRANSACTION_RESULT_CALLBACK, QueuedOperationResultCallback.Receiver.wrap(queuedOperationResultCallback));
        }
        SerializationUtils.putParcelable(queuedSiteToSiteClientConfig, intent, SITE_TO_SITE_CONFIG);
        return intent;
    }

    private static Intent getIntent(Context context, IntentType intentType, Iterable<DataPacket> packets, boolean completeWakefulIntent) {
        Intent intent = new Intent(context, SiteToSiteService.class);
        intent.putExtra(INTENT_TYPE, intentType.name());
        if (packets != null) {
            ArrayList<DataPacket> packetList = new ArrayList<>();
            for (DataPacket packet : packets) {
                packetList.add(packet);
            }
            intent.putParcelableArrayListExtra(DATA_PACKETS, packetList);
        }
        if (completeWakefulIntent) {
            intent.putExtra(SHOULD_COMPLETE_WAKEFUL_INTENT, true);
        }
        return intent;
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        PowerManager powerManager = (PowerManager) getSystemService(POWER_SERVICE);
        PowerManager.WakeLock wakeLock = powerManager.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, CANONICAL_NAME);
        wakeLock.acquire();

        try {
            if (intent.getBooleanExtra(SHOULD_COMPLETE_WAKEFUL_INTENT, false)) {
                WakefulBroadcastReceiver.completeWakefulIntent(intent);
            }
            IntentType intentType = IntentType.valueOf(intent.getStringExtra(INTENT_TYPE));
            Intent repeatingIntent = SerializationUtils.getParcelable(intent, SiteToSiteRepeating.REPEATING_INTENT);
            if (intentType.isQueueOperation()) {
                ResultReceiver queuedOperationResultCallback = intent.getExtras().getParcelable(TRANSACTION_RESULT_CALLBACK);
                QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig = SerializationUtils.getParcelable(intent, SITE_TO_SITE_CONFIG);
                try {
                    QueuedSiteToSiteClient queuedSiteToSiteClient = queuedSiteToSiteClientConfig.getQueuedSiteToSiteClient(getApplicationContext());
                    if (intentType == IntentType.ENQUEUE) {
                        List<DataPacket> packets = intent.getExtras().getParcelableArrayList(DATA_PACKETS);
                        if (packets.size() > 0) {
                            queuedSiteToSiteClient.enqueue(packets.iterator());
                        }
                    } else if (intentType == IntentType.PROCESS) {
                        queuedSiteToSiteClient.process();
                    } else if (intentType == IntentType.CLEANUP) {
                        queuedSiteToSiteClient.cleanup();
                    } else {
                        Log.e(CANONICAL_NAME, "Unexpected intent type: " + intentType);
                    }
                    SiteToSiteRepeating.updateIntentConfig(getApplicationContext(), repeatingIntent, queuedSiteToSiteClientConfig);
                    QueuedOperationResultCallback.Receiver.onSuccess(queuedOperationResultCallback, queuedSiteToSiteClientConfig);
                } catch (IOException e) {
                    Log.d(CANONICAL_NAME, "Performing queue operation.", e);
                    if (queuedOperationResultCallback != null) {
                        SiteToSiteRepeating.updateIntentConfig(getApplicationContext(), repeatingIntent, queuedSiteToSiteClientConfig);
                        QueuedOperationResultCallback.Receiver.onException(queuedOperationResultCallback, e, queuedSiteToSiteClientConfig);
                    }
                } finally {

                }
            } else if (intentType == IntentType.SEND) {
                List<DataPacket> packets = intent.getExtras().getParcelableArrayList(DATA_PACKETS);
                ResultReceiver transactionResultCallback = intent.getExtras().getParcelable(TRANSACTION_RESULT_CALLBACK);
                SiteToSiteClientConfig siteToSiteClientConfig = SerializationUtils.getParcelable(intent, SITE_TO_SITE_CONFIG);
                if (packets.size() > 0) {
                    try {
                        SiteToSiteClient client = siteToSiteClientConfig.createClient();
                        Transaction transaction = client.createTransaction();
                        for (DataPacket packet : packets) {
                            transaction.send(packet);
                        }
                        transaction.confirm();
                        TransactionResult transactionResult = transaction.complete();
                        if (transactionResultCallback != null) {
                            SiteToSiteRepeating.updateIntentConfig(getApplicationContext(), repeatingIntent, siteToSiteClientConfig);
                            TransactionResultCallback.Receiver.onSuccess(transactionResultCallback, transactionResult, siteToSiteClientConfig);
                        }
                    } catch (IOException e) {
                        Log.d(CANONICAL_NAME, "Error sending packets.", e);
                        if (transactionResultCallback != null) {
                            SiteToSiteRepeating.updateIntentConfig(getApplicationContext(), repeatingIntent, siteToSiteClientConfig);
                            TransactionResultCallback.Receiver.onException(transactionResultCallback, e, siteToSiteClientConfig);
                        }
                    }
                } else {
                    TransactionResultCallback.Receiver.onSuccess(transactionResultCallback, new TransactionResult(0, ResponseCode.CONFIRM_TRANSACTION, "No-op due to empty packet list."), siteToSiteClientConfig);
                }
            }
        } catch (Exception e) {
            Log.e(CANONICAL_NAME, "Unexpected error processing intent: " + intent, e);
        } finally {
            wakeLock.release();
        }
    }

    public enum IntentType {
        SEND(false), ENQUEUE(true), PROCESS(true), CLEANUP(true);

        private final boolean queueOperation;

        IntentType(boolean queueOperation) {
            this.queueOperation = queueOperation;
        }

        public boolean isQueueOperation() {
            return queueOperation;
        }
    }
}
