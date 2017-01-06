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

import android.app.AlarmManager;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Handler;
import android.os.Looper;
import android.os.SystemClock;
import android.preference.PreferenceManager;
import android.support.v7.app.AppCompatActivity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.EditText;
import android.widget.TextView;

import org.apache.nifi.android.sitetosite.client.peer.PeerStatus;
import org.apache.nifi.android.sitetosite.client.persistence.PendingIntentWrapper;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.persistence.TransactionLogEntry;
import org.apache.nifi.android.sitetositedemo.preference.SiteToSitePreferenceActivity;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.service.SiteToSiteRepeatableIntent;
import org.apache.nifi.android.sitetosite.service.SiteToSiteRepeating;
import org.apache.nifi.android.sitetosite.service.SiteToSiteService;
import org.apache.nifi.android.sitetosite.service.TransactionResultCallback;
import org.apache.nifi.android.sitetosite.util.Charsets;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;

public class MainActivity extends AppCompatActivity implements ScheduleDialogCallback {
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);
    private final Handler handler = new Handler(Looper.getMainLooper());
    private SiteToSiteDB siteToSiteDB;
    private long lastTimestamp = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        siteToSiteDB = new SiteToSiteDB(getApplicationContext());
        TextView sendResults = (TextView) findViewById(R.id.sendResults);
        sendResults.setMovementMethod(new ScrollingMovementMethod());
        sendResults.post(new Runnable() {
            @Override
            public void run() {
                getApplicationContext().registerReceiver(new BroadcastReceiver() {
                    @Override
                    public void onReceive(Context context, Intent intent) {
                        refresh();
                    }
                }, new IntentFilter(SiteToSiteDB.TRANSACTION_LOG_ENTRY_SAVED), null, handler);
                refresh();
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        getMenuInflater().inflate(R.menu.menu, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case R.id.preferences:
                Intent intent = new Intent();
                intent.setClassName(this, SiteToSitePreferenceActivity.class.getCanonicalName());
                startActivity(intent);
                return true;
        }

        return super.onOptionsItemSelected(item);
    }

    /**
     * Called when the user clicks the Send button
     */
    public void sendMessage(View view) {
        Map<String, String> attributes = new HashMap<>();
        ByteArrayDataPacket dataPacket = new ByteArrayDataPacket(attributes, ((EditText) findViewById(R.id.edit_message)).getText().toString().getBytes(Charsets.UTF_8));
        SiteToSiteService.sendDataPacket(getApplicationContext(), dataPacket, getClientConfig(), new TransactionResultCallback() {

            @Override
            public Handler getHandler() {
                return handler;
            }

            @Override
            public void onSuccess(TransactionResult transactionResult, SiteToSiteClientConfig siteToSiteClientConfig) {
                siteToSiteDB.save(new TransactionLogEntry(transactionResult));
                siteToSiteDB.save(siteToSiteClientConfig.getUrls(), siteToSiteClientConfig.getProxyHost(), siteToSiteClientConfig.getProxyPort(), siteToSiteClientConfig.getPeerStatus());
            }

            @Override
            public void onException(IOException exception, SiteToSiteClientConfig siteToSiteClientConfig) {
                siteToSiteDB.save(new TransactionLogEntry(exception));
                siteToSiteDB.save(siteToSiteClientConfig.getUrls(), siteToSiteClientConfig.getProxyHost(), siteToSiteClientConfig.getProxyPort(), siteToSiteClientConfig.getPeerStatus());
            }
        });
    }

    public void schedule(View view) {
        new ScheduleDialogFragment().show(getSupportFragmentManager(), "schedule");
    }

    private SiteToSiteClientConfig getClientConfig() {
        SharedPreferences preferences = PreferenceManager.getDefaultSharedPreferences(this);
        HashSet<String> peerUrls = new HashSet<>(Arrays.asList(preferences.getString("peer_urls_preference", "http://localhost:8080/nifi")));
        String proxyHost = preferences.getString("proxy_host_preference", "");
        int proxyPort = Integer.parseInt(preferences.getString("proxy_port_preference", "0"));

        SiteToSiteClientConfig siteToSiteClientConfig = new SiteToSiteClientConfig();
        siteToSiteClientConfig.setUrls(peerUrls);
        siteToSiteClientConfig.setPortName(preferences.getString("input_port_preference", null));
        siteToSiteClientConfig.setUsername(preferences.getString("username_preference", null));
        siteToSiteClientConfig.setPassword(preferences.getString("password_preference", null));
        siteToSiteClientConfig.setProxyHost(proxyHost);
        siteToSiteClientConfig.setProxyPort(proxyPort);
        siteToSiteClientConfig.setProxyUsername(preferences.getString("proxy_port_username", null));
        siteToSiteClientConfig.setProxyPassword(preferences.getString("proxy_port_password", null));

        PeerStatus peerStatus = siteToSiteDB.getPeerStatus(peerUrls, proxyHost, proxyPort);
        if (peerStatus != null) {
            siteToSiteClientConfig.setPeerStatus(peerStatus);
        }
        return siteToSiteClientConfig;
    }

    private void refresh() {
        final TextView resultView = (TextView) findViewById(R.id.sendResults);
        for (TransactionLogEntry transactionLogEntry : siteToSiteDB.getLogEntries(lastTimestamp)) {
            StringBuilder stringBuilder = new StringBuilder(LINE_SEPARATOR);
            stringBuilder.append("[");
            stringBuilder.append(simpleDateFormat.format(transactionLogEntry.getCreated()));
            stringBuilder.append("] - ");

            TransactionResult transactionResult = transactionLogEntry.getTransactionResult();
            if (transactionResult != null) {
                stringBuilder.append("Sent ");
                stringBuilder.append(transactionResult.getFlowFilesSent());
                stringBuilder.append(" flow file(s) and received response \"");
                stringBuilder.append(transactionResult.getResponseCode());
                stringBuilder.append("\"");
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
        SiteToSiteRepeatableIntent siteToSiteRepeatableIntent = SiteToSiteRepeating.createPendingIntent(getApplicationContext(), new TestDataCollector(((EditText) findViewById(R.id.edit_message)).getText().toString()), getClientConfig(), new RepeatingTransactionResultCallback());
        siteToSiteDB.save(siteToSiteRepeatableIntent);
        ((AlarmManager) getApplicationContext().getSystemService(Context.ALARM_SERVICE)).setInexactRepeating(AlarmManager.ELAPSED_REALTIME_WAKEUP, SystemClock.elapsedRealtime(), intervalMillis, siteToSiteRepeatableIntent.getPendingIntent());
    }

    public void cancelAlarms(View view) {
        AlarmManager alarmManager = (AlarmManager) getApplicationContext().getSystemService(Context.ALARM_SERVICE);
        for (PendingIntentWrapper pendingIntentWrapper : siteToSiteDB.getPendingIntents()) {
            PendingIntent pendingIntent = pendingIntentWrapper.getPendingIntent();
            if (pendingIntent != null) {
                alarmManager.cancel(pendingIntent);
            }
            siteToSiteDB.deletePendingIntent(pendingIntentWrapper.getRowId());
        }
    }
}
