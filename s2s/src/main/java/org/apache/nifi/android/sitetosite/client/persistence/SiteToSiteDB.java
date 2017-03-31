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

package org.apache.nifi.android.sitetosite.client.persistence;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Parcel;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClientConfig;
import org.apache.nifi.android.sitetosite.client.peer.PeerStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Easily save and load state useful for site-to-site communication
 */
public class SiteToSiteDB {
    public static final String TRANSACTION_LOG_ENTRY_SAVED = SiteToSiteDB.class.getCanonicalName() + ".save(transactionLogEntry)";
    public static final String ID_COLUMN = "ID";
    public static final String CONTENT_COLUMN = "CONTENT";
    public static final String CREATED_COLUMN = "CREATED";
    public static final String EXPIRATION_MILLIS_COLUMN = "EXPIRATION_MILLIS";

    public static final String PEER_STATUSES_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_PEER_STATUSES";
    public static final String PEER_STATUS_URLS_COLUMN = "URLS";
    public static final String PEER_STATUS_PROXY_HOST_COLUMN = "PROXY_HOST";
    public static final String PEER_STATUS_PROXY_PORT_COLUMN = "PROXY_PORT";
    public static final String PEER_STATUS_WHERE_CLAUSE = PEER_STATUS_URLS_COLUMN + " = ? AND " + PEER_STATUS_PROXY_HOST_COLUMN + " = ? AND " + PEER_STATUS_PROXY_PORT_COLUMN + " = ?";

    public static final String DATA_PACKET_QUEUE_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_QUEUE";
    public static final String DATA_PACKET_QEUE_PRIORITY_COLUMN = "PRIORITY";
    public static final String DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN = "ATTRIBUTES";
    public static final String DATA_PACKET_QUEUE_TRANSACTION_COLUMN = "TRANSACTION_ID";

    public static final String DATA_PACKET_QUEUE_TRANSACTIONS_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_QUEUE_TRANSACTIONS";

    public static final int VERSION = 1;

    private static SQLiteOpenHelper sqLiteOpenHelper;

    private final Context context;

    public SiteToSiteDB(Context context) {
        this.context = context;
        synchronized (SiteToSiteDB.class) {
            if (sqLiteOpenHelper == null) {
                sqLiteOpenHelper = new SQLiteOpenHelper(context, SiteToSiteDB.class.getSimpleName() + ".db", null, VERSION) {
                    @Override
                    public void onCreate(SQLiteDatabase db) {
                        db.execSQL("CREATE TABLE " + PEER_STATUSES_TABLE_NAME + " (" +
                                PEER_STATUS_URLS_COLUMN + " TEXT, " +
                                PEER_STATUS_PROXY_HOST_COLUMN + " TEXT, " +
                                PEER_STATUS_PROXY_PORT_COLUMN + " INTEGER, " +
                                CONTENT_COLUMN + " BLOB, " +
                                EXPIRATION_MILLIS_COLUMN + " INTEGER, " +
                                "PRIMARY KEY(" + PEER_STATUS_URLS_COLUMN + ", " + PEER_STATUS_PROXY_HOST_COLUMN + ", " + PEER_STATUS_PROXY_PORT_COLUMN + "))");
                        db.execSQL("CREATE INDEX " + PEER_STATUSES_TABLE_NAME + "_" + EXPIRATION_MILLIS_COLUMN + "_index ON " + PEER_STATUSES_TABLE_NAME + "(" + EXPIRATION_MILLIS_COLUMN + ")");

                        db.execSQL("CREATE TABLE " + DATA_PACKET_QUEUE_TABLE_NAME + "(" +
                                ID_COLUMN + " INTEGER PRIMARY KEY, " +
                                CREATED_COLUMN + " INTEGER, " +
                                DATA_PACKET_QEUE_PRIORITY_COLUMN + " INTEGER, " +
                                DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN + " BLOB, " +
                                CONTENT_COLUMN + " BLOB, " +
                                DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " INTEGER, " +
                                EXPIRATION_MILLIS_COLUMN + " INTEGER)");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_" + DATA_PACKET_QUEUE_TRANSACTION_COLUMN + "_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + DATA_PACKET_QUEUE_TRANSACTION_COLUMN + ")");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_" + EXPIRATION_MILLIS_COLUMN + "_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + EXPIRATION_MILLIS_COLUMN + ")");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_sort_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + DATA_PACKET_QEUE_PRIORITY_COLUMN + ", " + CREATED_COLUMN + ", " + ID_COLUMN + ")");

                        db.execSQL("CREATE TABLE " + DATA_PACKET_QUEUE_TRANSACTIONS_TABLE_NAME + "(" +
                                DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " INTEGER PRIMARY KEY, " +
                                EXPIRATION_MILLIS_COLUMN + " INTEGER)");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TRANSACTIONS_TABLE_NAME + "_" + EXPIRATION_MILLIS_COLUMN + "_index ON " + DATA_PACKET_QUEUE_TRANSACTIONS_TABLE_NAME + "(" + EXPIRATION_MILLIS_COLUMN + ")");
                    }

                    @Override
                    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

                    }
                };
            }
        }
    }

    /**
     * Saves the peer status for a given url set and proxy
     *
     * @param siteToSiteClientConfig the configuration to save the peer status for
     */
    public void savePeerStatus(SiteToSiteClientConfig siteToSiteClientConfig) throws SQLiteIOException {
        PeerStatus peerStatus = siteToSiteClientConfig.getPeerStatus();
        if (peerStatus == null) {
            return;
        }

        Parcel parcel = Parcel.obtain();
        peerStatus.writeToParcel(parcel, 0);
        parcel.setDataPosition(0);
        byte[] bytes = parcel.marshall();

        String urlsString = getPeerUrlsString(siteToSiteClientConfig.getUrls());
        String proxyHost = siteToSiteClientConfig.getProxyHost();
        int proxyPort = siteToSiteClientConfig.getProxyPort();

        SQLiteDatabase writableDatabase = sqLiteOpenHelper.getWritableDatabase();
        writableDatabase.beginTransaction();
        try {
            try {
                writableDatabase.execSQL("DELETE FROM " + PEER_STATUSES_TABLE_NAME + " WHERE " + EXPIRATION_MILLIS_COLUMN + " <= ?", new Object[]{new Date().getTime()});
                writableDatabase.delete(PEER_STATUSES_TABLE_NAME, PEER_STATUS_WHERE_CLAUSE, new String[]{urlsString, proxyHost, Integer.toString(proxyPort)});
                ContentValues values = new ContentValues();
                values.put(PEER_STATUS_URLS_COLUMN, urlsString);
                if (proxyHost == null || proxyHost.isEmpty()) {
                    values.putNull(PEER_STATUS_PROXY_HOST_COLUMN);
                    values.putNull(PEER_STATUS_PROXY_PORT_COLUMN);
                } else {
                    values.put(PEER_STATUS_PROXY_HOST_COLUMN, proxyHost);
                    values.put(PEER_STATUS_PROXY_PORT_COLUMN, proxyPort);
                }
                values.put(CONTENT_COLUMN, bytes);
                values.put(EXPIRATION_MILLIS_COLUMN, new Date().getTime() + siteToSiteClientConfig.getPeerUpdateInterval(TimeUnit.MILLISECONDS));
                writableDatabase.insertOrThrow(PEER_STATUSES_TABLE_NAME, null, values);
                writableDatabase.setTransactionSuccessful();
            } finally {
                writableDatabase.endTransaction();
            }
        } catch (SQLiteException e) {
            throw new SQLiteIOException("Unable to store peer status in database.", e);
        } finally {
            writableDatabase.close();
        }
    }

    /**
     * Gets the peer status for a given url set and proxy
     *
     * @param siteToSiteClientConfig the config to get peer status for
     */
    public void updatePeerStatusOnConfig(SiteToSiteClientConfig siteToSiteClientConfig) throws SQLiteIOException {
        PeerStatus origPeerStatus = siteToSiteClientConfig.getPeerStatus();

        List<String> parameters = new ArrayList<>();

        String peerUrlsString = getPeerUrlsString(siteToSiteClientConfig.getUrls());
        StringBuilder queryString = new StringBuilder(PEER_STATUS_URLS_COLUMN).append(" = ? AND ").append(PEER_STATUS_PROXY_HOST_COLUMN);
        parameters.add(peerUrlsString);

        String proxyHost = siteToSiteClientConfig.getProxyHost();
        if (proxyHost == null || proxyHost.isEmpty()) {
            queryString.append(" IS NULL AND ").append(PEER_STATUS_PROXY_PORT_COLUMN).append(" IS NULL");
        } else {
            queryString.append(" = ? AND ").append(PEER_STATUS_PROXY_PORT_COLUMN).append(" = ?");
            parameters.add(proxyHost);
            parameters.add(Integer.toString(siteToSiteClientConfig.getProxyPort()));
        }

        SQLiteDatabase readableDatabase = sqLiteOpenHelper.getReadableDatabase();
        try {
            Cursor cursor;
            cursor = readableDatabase.query(false, PEER_STATUSES_TABLE_NAME, new String[]{CONTENT_COLUMN}, queryString.toString(),
                    parameters.toArray(new String[parameters.size()]), null, null, null, null);
            try {
                int contentIndex = cursor.getColumnIndexOrThrow(CONTENT_COLUMN);
                while (cursor.moveToNext()) {
                    byte[] bytes = cursor.getBlob(contentIndex);
                    Parcel parcel = Parcel.obtain();
                    parcel.unmarshall(bytes, 0, bytes.length);
                    parcel.setDataPosition(0);
                    PeerStatus dbPeerStatus = PeerStatus.CREATOR.createFromParcel(parcel);
                    if (dbPeerStatus != null && (origPeerStatus == null || origPeerStatus.getLastPeerUpdate() < dbPeerStatus.getLastPeerUpdate())) {
                        siteToSiteClientConfig.setPeerStatus(dbPeerStatus);
                        origPeerStatus = dbPeerStatus;
                    }
                }
            } finally {
                cursor.close();
            }
        } catch (SQLiteException e) {
            throw new SQLiteIOException("Unable to read peer status from database.", e);
        } finally {
            readableDatabase.close();
        }
    }

    private String getPeerUrlsString(Set<String> peerUrlsPreference) {
        List<String> orderedUrls = new ArrayList<>(peerUrlsPreference);
        Collections.sort(orderedUrls);
        StringBuilder stringBuilder = new StringBuilder();
        for (String orderedUrl : orderedUrls) {
            stringBuilder.append(orderedUrl);
            stringBuilder.append(",");
        }
        if (orderedUrls.size() > 0) {
            stringBuilder.setLength(stringBuilder.length() - 1);
        }
        return stringBuilder.toString();
    }

    /**
     * Returns a readable sqlite db
     *
     * @return a readable sqlite db
     */
    public SQLiteDatabase getReadableDatabase() {
        return sqLiteOpenHelper.getReadableDatabase();
    }

    /**
     * Returns a writable sqlite db
     *
     * @return a writable sqlite db
     */
    public SQLiteDatabase getWritableDatabase() {
        return sqLiteOpenHelper.getWritableDatabase();
    }
}
