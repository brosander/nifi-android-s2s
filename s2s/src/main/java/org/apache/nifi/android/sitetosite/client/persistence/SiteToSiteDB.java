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

import android.app.PendingIntent;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.os.Parcel;

import org.apache.nifi.android.sitetosite.client.peer.PeerStatus;
import org.apache.nifi.android.sitetosite.service.SiteToSiteRepeatableIntent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Easily save and load state useful for site-to-site communication
 */
public class SiteToSiteDB {
    public static final String TRANSACTION_LOG_ENTRY_SAVED = SiteToSiteDB.class.getCanonicalName() + ".save(transactionLogEntry)";
    public static final String ID_COLUMN = "ID";
    public static final String CONTENT_COLUMN = "CONTENT";
    public static final String CREATED_COLUMN = "CREATED";

    public static final String PEER_STATUSES_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_PEER_STATUSES";
    public static final String PEER_STATUS_URLS_COLUMN = "URLS";
    public static final String PEER_STATUS_PROXY_HOST_COLUMN = "PROXY_HOST";
    public static final String PEER_STATUS_PROXY_PORT_COLUMN = "PROXY_PORT";
    public static final String PEER_STATUS_WHERE_CLAUSE = PEER_STATUS_URLS_COLUMN + " = ? AND " + PEER_STATUS_PROXY_HOST_COLUMN + " = ? AND " + PEER_STATUS_PROXY_PORT_COLUMN + " = ?";

    public static final String DATA_PACKET_QUEUE_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_QUEUE";
    public static final String DATA_PACKET_QEUE_PRIORITY_COLUMN = "PRIORITY";
    public static final String DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN = "ATTRIBUTES";
    public static final String DATA_PACKET_QUEUE_TRANSACTION_COLUMN = "TRANSACTION_ID";
    public static final String DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN = "EXPIRATION_MILLIS";

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
                                "PRIMARY KEY(" + PEER_STATUS_URLS_COLUMN + ", " + PEER_STATUS_PROXY_HOST_COLUMN + ", " + PEER_STATUS_PROXY_PORT_COLUMN + "))");
                        db.execSQL("CREATE TABLE " + DATA_PACKET_QUEUE_TABLE_NAME + "(" +
                                ID_COLUMN + " INTEGER PRIMARY KEY, " +
                                CREATED_COLUMN + " INTEGER, " +
                                DATA_PACKET_QEUE_PRIORITY_COLUMN + " INTEGER, " +
                                DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN + " BLOB, " +
                                CONTENT_COLUMN + " BLOB, " +
                                DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " TEXT, " +
                                DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN + " INTEGER)");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_" + DATA_PACKET_QUEUE_TRANSACTION_COLUMN + "_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + DATA_PACKET_QUEUE_TRANSACTION_COLUMN + ")");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_" + DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN + "_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN + ")");
                        db.execSQL("CREATE INDEX " + DATA_PACKET_QUEUE_TABLE_NAME + "_sort_index ON " + DATA_PACKET_QUEUE_TABLE_NAME + "(" + DATA_PACKET_QEUE_PRIORITY_COLUMN + ", " + CREATED_COLUMN + ", " + ID_COLUMN + ")");
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
     * @param peerUrlsPreference the configured peer urls
     * @param proxyHost the proxy host
     * @param proxyPort the proxy port
     * @param peerStatus the peer status to save
     */
    public void save(Set<String> peerUrlsPreference, String proxyHost, int proxyPort, PeerStatus peerStatus) {
        if (peerStatus == null) {
            return;
        }

        String urlsString = getPeerUrlsString(peerUrlsPreference);

        Parcel parcel = Parcel.obtain();
        peerStatus.writeToParcel(parcel, 0);
        parcel.setDataPosition(0);
        byte[] bytes = parcel.marshall();

        SQLiteDatabase writableDatabase = sqLiteOpenHelper.getWritableDatabase();
        writableDatabase.beginTransaction();
        try {
            try {
                writableDatabase.delete(PEER_STATUSES_TABLE_NAME, PEER_STATUS_WHERE_CLAUSE, new String[]{urlsString, proxyHost, Integer.toString(proxyPort)});
                ContentValues values = new ContentValues();
                values.put(PEER_STATUS_URLS_COLUMN, urlsString);
                values.put(PEER_STATUS_PROXY_HOST_COLUMN, proxyHost);
                values.put(PEER_STATUS_PROXY_PORT_COLUMN, proxyPort);
                values.put(CONTENT_COLUMN, bytes);
                writableDatabase.insertOrThrow(PEER_STATUSES_TABLE_NAME, null, values);
                writableDatabase.setTransactionSuccessful();
            } finally {
                writableDatabase.endTransaction();
            }
        } finally {
            writableDatabase.close();
        }
    }

    /**
     * Gets the peer status for a given url set and proxy
     *
     * @param peerUrlsPreference the configured urls
     * @param proxyHost the proxy host
     * @param proxyPort the proxy port
     * @return the peer status
     */
    public PeerStatus getPeerStatus(Set<String> peerUrlsPreference, String proxyHost, int proxyPort) {
        String peerUrlsString = getPeerUrlsString(peerUrlsPreference);

        SQLiteDatabase readableDatabase = sqLiteOpenHelper.getReadableDatabase();
        try {
            Cursor cursor = readableDatabase.query(false, PEER_STATUSES_TABLE_NAME, new String[]{CONTENT_COLUMN}, PEER_STATUS_WHERE_CLAUSE,
                    new String[]{peerUrlsString, proxyHost, Integer.toString(proxyPort)}, null, null, null, null);
            try {
                int contentIndex = cursor.getColumnIndexOrThrow(CONTENT_COLUMN);
                while (cursor.moveToNext()) {
                    byte[] bytes = cursor.getBlob(contentIndex);
                    Parcel parcel = Parcel.obtain();
                    parcel.unmarshall(bytes, 0, bytes.length);
                    parcel.setDataPosition(0);
                    return PeerStatus.CREATOR.createFromParcel(parcel);
                }
            } finally {
                cursor.close();
            }
        } finally {
            readableDatabase.close();
        }
        return null;
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
