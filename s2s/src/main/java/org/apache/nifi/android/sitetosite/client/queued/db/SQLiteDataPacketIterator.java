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

package org.apache.nifi.android.sitetosite.client.queued.db;

import android.content.ContentValues;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.util.Log;

import org.apache.nifi.android.sitetosite.client.persistence.SQLiteIOException;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.CONTENT_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.CREATED_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QEUE_PRIORITY_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.EXPIRATION_MILLIS_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_TABLE_NAME;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_TRANSACTION_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.ID_COLUMN;

public class SQLiteDataPacketIterator {
    public static final String CANONICAL_NAME = SQLiteDataPacketIterator.class.getCanonicalName();
    public static final String MARK_ROWS_FOR_TRANSACTION_QUERY = new StringBuilder("UPDATE ").append(DATA_PACKET_QUEUE_TABLE_NAME)
            .append(" SET ").append(DATA_PACKET_QUEUE_TRANSACTION_COLUMN).append(" = ?")
            .append(" WHERE ").append(ID_COLUMN).append(" IN ")
            .append("(SELECT ").append(ID_COLUMN).append(" FROM ").append(DATA_PACKET_QUEUE_TABLE_NAME)
            .append(" WHERE ").append(EXPIRATION_MILLIS_COLUMN).append(" > ?")
            .append(" AND ").append(DATA_PACKET_QUEUE_TRANSACTION_COLUMN).append(" IS NULL")
            .append(" ORDER BY ").append(DATA_PACKET_QEUE_PRIORITY_COLUMN).append(" DESC, ").append(CREATED_COLUMN).append(" DESC, ").append(ID_COLUMN).append(" DESC")
            .append(" LIMIT ?)").toString();

    private final SiteToSiteDB siteToSiteDB;
    private final String transactionId;
    private final SQLiteDatabase readableDatabase;
    private final Cursor cursor;
    private final int attributesIndex;
    private final int contentIndex;
    private boolean hasNext;

    public SQLiteDataPacketIterator(SiteToSiteDB siteToSiteDB, String transactionId, int limit) throws SQLiteIOException {
        this.siteToSiteDB = siteToSiteDB;
        this.transactionId = transactionId;
        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            writableDatabase.execSQL(MARK_ROWS_FOR_TRANSACTION_QUERY, new Object[] {transactionId, new Date().getTime(), limit});
        } finally {
            writableDatabase.close();
        }
        this.readableDatabase = siteToSiteDB.getReadableDatabase();
        Cursor cursor = null;
        try {
            cursor = readableDatabase.query(false, DATA_PACKET_QUEUE_TABLE_NAME, new String[]{DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN, CONTENT_COLUMN},
                    DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " = ?", new String[]{transactionId}, null, null,
                    DATA_PACKET_QEUE_PRIORITY_COLUMN + " DESC, " + CREATED_COLUMN + " DESC, " + ID_COLUMN + " DESC", null);
            this.cursor = cursor;
            this.attributesIndex = cursor.getColumnIndex(DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN);
            this.contentIndex = cursor.getColumnIndex(CONTENT_COLUMN);
        } catch (SQLiteException e){
            if (cursor != null) {
                cursor.close();
            }
            readableDatabase.close();
            throw new SQLiteIOException("Unable to query " + DATA_PACKET_QUEUE_TABLE_NAME + " for queued packets.", e);
        }
        hasNext = cursor.moveToNext();
        if (!hasNext) {
            close();
        }
    }

    public boolean hasNext() {
        return hasNext;
    }

    public DataPacket next() {
        String json = new String(cursor.getBlob(attributesIndex), Charsets.UTF_8);
        Map<String, String> attributes = new HashMap<>();
        try {
            JSONObject attributesObject = new JSONObject(json);
            Iterator<String> keys = attributesObject.keys();
            while (keys.hasNext()) {
                String name = keys.next();
                attributes.put(name, attributesObject.getString(name));
            }
        } catch (JSONException e) {
            Log.w(CANONICAL_NAME, "JSON errors shouldn't happen here as same library was responsible for inserting well-formed JSON: " + json, e);
        }
        byte[] data = cursor.getBlob(contentIndex);
        hasNext = cursor.moveToNext();
        return new ByteArrayDataPacket(attributes, data);
    }

    public void transactionComplete() throws SQLiteIOException {
        close();
        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            writableDatabase.delete(DATA_PACKET_QUEUE_TABLE_NAME, DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " = ?", new String[] {transactionId});
        } catch (SQLiteException e) {
            throw new SQLiteIOException("Unable to delete sent data packets, data may be duplicated.", e);
        } finally {
            writableDatabase.close();
        }
    }

    public void transactionFailed() throws SQLiteIOException {
        close();
        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            ContentValues contentValues = new ContentValues();
            contentValues.putNull(DATA_PACKET_QUEUE_TRANSACTION_COLUMN);
            writableDatabase.update(DATA_PACKET_QUEUE_TABLE_NAME, contentValues, DATA_PACKET_QUEUE_TRANSACTION_COLUMN + " = ?", new String[] {transactionId});
        } catch (SQLiteException e) {
            throw new SQLiteIOException("Unable to clear transaction from failed data packets.", e);
        } finally {
            writableDatabase.close();
        }
    }

    private void close() {
        cursor.close();
        readableDatabase.close();
    }
}
