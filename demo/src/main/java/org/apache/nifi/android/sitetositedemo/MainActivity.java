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

import android.annotation.SuppressLint;
import android.app.AlarmManager;
import android.app.PendingIntent;
import android.app.job.JobInfo;
import android.app.job.JobScheduler;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.os.Build;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.support.annotation.RequiresApi;
import android.support.v7.app.AppCompatActivity;
import android.text.method.ScrollingMovementMethod;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.factory.PropertiesQueuedSiteToSiteClientConfigFactory;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.service.QueuedOperationResultCallback;
import org.apache.nifi.android.sitetosite.service.SiteToSiteJobService;
import org.apache.nifi.android.sitetosite.service.SiteToSiteRepeatableIntent;
import org.apache.nifi.android.sitetosite.service.SiteToSiteRepeating;
import org.apache.nifi.android.sitetosite.service.SiteToSiteService;
import org.apache.nifi.android.sitetosite.service.TransactionResultCallback;
import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

public class MainActivity extends AppCompatActivity implements ScheduleDialogCallback {
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    private final Handler handler = new Handler(Looper.getMainLooper());
    private DemoAppDB demoAppDB;
    private QueuedSiteToSiteClientConfig siteToSiteClientConfig;
    private long lastTimestamp = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        demoAppDB = new DemoAppDB(getApplicationContext());
        final TextView sendResults = (TextView) findViewById(R.id.sendResults);
        sendResults.setMovementMethod(new ScrollingMovementMethod());

        Properties properties = new Properties();
        final String s2sConfigName = "simple.properties";
        InputStream resourceAsStream = MainActivity.class.getClassLoader().getResourceAsStream(s2sConfigName);
        try {
            properties.load(resourceAsStream);
            siteToSiteClientConfig = new PropertiesQueuedSiteToSiteClientConfigFactory().create(properties);
        } catch (final Exception e) {
            sendResults.post(new Runnable() {
                @SuppressLint("SetTextI18n")
                @Override
                public void run() {
                    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                    PrintStream printStream = new PrintStream(byteArrayOutputStream);
                    try {
                        e.printStackTrace(printStream);
                    } finally {
                        printStream.close();
                    }
                    sendResults.setText(e.getMessage() + "\n" + new String(byteArrayOutputStream.toByteArray(), Charsets.UTF_8) + "\n\n" + "Unable to load siteToSite configuration from " + s2sConfigName);
                }
            });
            return;
        } finally {
            try {
                resourceAsStream.close();
            } catch (IOException e) {
                // Ignore
            }
        }
        sendResults.post(new Runnable() {
            @Override
            public void run() {
                getApplicationContext().registerReceiver(new BroadcastReceiver() {
                    @Override
                    public void onReceive(Context context, Intent intent) {
                        refresh();
                    }
                }, new IntentFilter(DemoAppDB.TRANSACTION_LOG_ENTRY_SAVED), null, handler);
                refresh();
            }
        });
    }

    /**
     * Called when the user clicks the Send button
     */
    public void sendMessage(View view) {
        Map<String, String> attributes = new HashMap<>();
        ByteArrayDataPacket dataPacket = new ByteArrayDataPacket(attributes, ((EditText) findViewById(R.id.edit_message)).getText().toString().getBytes(Charsets.UTF_8));
        SiteToSiteService.sendDataPacket(getApplicationContext(), dataPacket, siteToSiteClientConfig, new TransactionResultCallback() {

            @Override
            public Handler getHandler() {
                return handler;
            }

            @Override
            public void onSuccess(TransactionResult transactionResult) {
                demoAppDB.save(new TransactionLogEntry(transactionResult));
            }

            @Override
            public void onException(IOException exception) {
                demoAppDB.save(new TransactionLogEntry(exception));
            }
        });
    }

    public void schedule(View view) {
        new ScheduleDialogFragment().show(getSupportFragmentManager(), "schedule");
    }

    private void refresh() {
        final TextView resultView = (TextView) findViewById(R.id.sendResults);
        for (TransactionLogEntry transactionLogEntry : demoAppDB.getLogEntries(lastTimestamp)) {
            StringBuilder stringBuilder = new StringBuilder(LINE_SEPARATOR);
            stringBuilder.append("[");
            stringBuilder.append(simpleDateFormat.format(transactionLogEntry.getCreated()));
            stringBuilder.append("] - ");

            TransactionResult transactionResult = transactionLogEntry.getTransactionResult();
            if (transactionResult != null) {
                if (transactionResult.getFlowFilesSent() < 0) {
                    stringBuilder.append(transactionResult.getMessage());
                } else {
                    stringBuilder.append("Sent ");
                    stringBuilder.append(transactionResult.getFlowFilesSent());
                    stringBuilder.append(" flow file(s) and received response \"");
                    stringBuilder.append(transactionResult.getResponseCode());
                    stringBuilder.append("\"");
                }
            } else {
                IOException ioException = transactionLogEntry.getIoException();
                if (ioException != null) {
                    StringWriter stringWriter = new StringWriter();
                    try {
                        PrintWriter printWriter = new PrintWriter(stringWriter);
                        try {
                            ioException.printStackTrace(printWriter);
                        } finally {
                            printWriter.close();
                        }
                    } finally {
                        try {
                            stringWriter.close();
                        } catch (IOException e) {
                            //Ignore
                        }
                    }
                    stringBuilder.append(stringWriter);
                } else {
                    stringBuilder.append("Error, no transaction result or exception");
                }
            }
            stringBuilder.append(LINE_SEPARATOR);
            lastTimestamp = Math.max(lastTimestamp, transactionLogEntry.getCreated().getTime());
            resultView.append(stringBuilder.toString());
        }
    }

    @Override
    public void onConfirm(long intervalMillis) {
        SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createSendPendingIntent(getApplicationContext(), new TestDataCollector(((EditText) findViewById(R.id.edit_message)).getText().toString()), siteToSiteClientConfig, new RepeatingTransactionResultCallback());
        demoAppDB.save(siteToSiteRepeatableIntent);
        ((AlarmManager) getApplicationContext().getSystemService(Context.ALARM_SERVICE)).setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), intervalMillis, siteToSiteRepeatableIntent.getPendingIntent());
    }

    public void cancelAlarms(View view) {
        AlarmManager alarmManager = (AlarmManager) getApplicationContext().getSystemService(Context.ALARM_SERVICE);
        for (PendingIntentWrapper pendingIntentWrapper : demoAppDB.getPendingIntents()) {
            PendingIntent pendingIntent = pendingIntentWrapper.getPendingIntent();
            if (pendingIntent != null) {
                alarmManager.cancel(pendingIntent);
            }
            demoAppDB.deletePendingIntent(pendingIntentWrapper.getRowId());
        }
    }

    public void processMessages(View view) {
        SiteToSiteService.processQueuedPackets(getApplicationContext(), siteToSiteClientConfig, new QueuedOperationResultCallback() {

            @Override
            public Handler getHandler() {
                return handler;
            }

            @Override
            public void onSuccess() {
                demoAppDB.save(new TransactionLogEntry(new TransactionResult(-1, ResponseCode.CONFIRM_TRANSACTION, "Successfully processed queued flow file(s).")));
            }

            @Override
            public void onException(IOException exception) {
                demoAppDB.save(new TransactionLogEntry(exception));
            }
        });
    }

    public void enqueueMessage(View view) {
        Map<String, String> attributes = new HashMap<>();
        ByteArrayDataPacket dataPacket = new ByteArrayDataPacket(attributes, ((EditText) findViewById(R.id.edit_message)).getText().toString().getBytes(Charsets.UTF_8));
        SiteToSiteService.enqueueDataPacket(getApplicationContext(), dataPacket, siteToSiteClientConfig, new QueuedOperationResultCallback() {

            @Override
            public Handler getHandler() {
                return handler;
            }

            @Override
            public void onSuccess() {
                demoAppDB.save(new TransactionLogEntry(new TransactionResult(-1, ResponseCode.CONFIRM_TRANSACTION, "Successfully enqueued queued flow file.")));
            }

            @Override
            public void onException(IOException exception) {
                demoAppDB.save(new TransactionLogEntry(exception));
            }
        });
    }

    @RequiresApi(api = Build.VERSION_CODES.LOLLIPOP)
    public void jobSchedule(View view) {
        JobInfo.Builder processJobInfoBuilder = SiteToSiteJobService.createProcessJobInfoBuilder(getApplicationContext(), 1234, siteToSiteClientConfig, new JobSchedulerCallback());
        processJobInfoBuilder.setRequiredNetworkType(JobInfo.NETWORK_TYPE_UNMETERED);
        processJobInfoBuilder.setRequiresCharging(true);
        JobScheduler jobScheduler = (JobScheduler) getApplicationContext().getSystemService(Context.JOB_SCHEDULER_SERVICE);
        jobScheduler.schedule(processJobInfoBuilder.build());
    }
}
