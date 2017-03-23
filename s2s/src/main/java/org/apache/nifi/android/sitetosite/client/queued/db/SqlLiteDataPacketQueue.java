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
import android.database.sqlite.SQLiteStatement;
import android.util.Log;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.queue.DataPacketPrioritizer;
import org.apache.nifi.android.sitetosite.client.queued.AbstractQueuedSiteToSiteClient;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.apache.nifi.android.sitetosite.util.IOUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.CONTENT_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.CREATED_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QEUE_PRIORITY_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.DATA_PACKET_QUEUE_TABLE_NAME;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.ID_COLUMN;

public class SqlLiteDataPacketQueue extends AbstractQueuedSiteToSiteClient {
    public static final String CANONICAL_NAME = SqlLiteDataPacketQueue.class.getCanonicalName();
    public static final String AGE_OFF_ROW_COUNT_QUERY = new StringBuilder("DELETE FROM ").append(DATA_PACKET_QUEUE_TABLE_NAME)
            .append(" WHERE ").append(ID_COLUMN)
            .append(" IN (SELECT ").append(ID_COLUMN)
            .append(" FROM ").append(DATA_PACKET_QUEUE_TABLE_NAME)
            .append(" ORDER BY ").append(DATA_PACKET_QEUE_PRIORITY_COLUMN).append(" ASC, ").append(CREATED_COLUMN).append(" ASC")
            .append(" LIMIT ?)").toString();

    private final SiteToSiteClient siteToSiteClient;
    private final SiteToSiteDB siteToSiteDB;
    private final DataPacketPrioritizer dataPacketPrioritizer;
    private final int limit;
    private final long softAgeOffAgeMillis;

    public SqlLiteDataPacketQueue(SiteToSiteClient siteToSiteClient, SiteToSiteDB siteToSiteDB, DataPacketPrioritizer dataPacketPrioritizer, int limit, long softAgeOffAgeMillis) {
        this.siteToSiteClient = siteToSiteClient;
        this.siteToSiteDB = siteToSiteDB;
        this.dataPacketPrioritizer = dataPacketPrioritizer;
        this.limit = limit;
        this.softAgeOffAgeMillis = softAgeOffAgeMillis;
    }

    private static void ageOffRowCount(SQLiteDatabase writableDatabase, long maxRows) {
        if (maxRows > 0) {
            Cursor cursor = writableDatabase.query(DATA_PACKET_QUEUE_TABLE_NAME, new String[]{"count(*) as rows"}, null, null, null, null, null);
            long rows;
            try {
                if (!cursor.moveToNext()) {
                    return;
                }
                rows = cursor.getLong(cursor.getColumnIndex("rows"));
            } finally {
                cursor.close();
            }
            if (rows > maxRows) {
                writableDatabase.execSQL(AGE_OFF_ROW_COUNT_QUERY, new Object[]{rows - maxRows});
            }
        }
    }

    private static void ageOffSize(SQLiteDatabase writableDatabase, long maxSize) {
        if (maxSize > 0) {
            Cursor cursor = null;
            try {
                cursor = writableDatabase.query(DATA_PACKET_QUEUE_TABLE_NAME,
                        new String[]{"sum(length(" + DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN + ")) as attributesSize", "sum(length(" + CONTENT_COLUMN + ")) as contentSize"}, null, null, null, null, null);
                if (!cursor.moveToNext()) {
                    return;
                }
                long currentSize = cursor.getLong(cursor.getColumnIndex("attributesSize")) + cursor.getLong(cursor.getColumnIndex("contentSize"));
                cursor.close();

                while (currentSize > maxSize) {
                    cursor = writableDatabase.query(DATA_PACKET_QUEUE_TABLE_NAME, new String[]{ID_COLUMN, "length(" + DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN + ") + length(" + CONTENT_COLUMN + ") as rowSize"},
                            null, null, null, null, DATA_PACKET_QEUE_PRIORITY_COLUMN + " ASC, " + CREATED_COLUMN + " ASC");

                    int idIndex = cursor.getColumnIndex(ID_COLUMN);
                    int rowSizeIndex = cursor.getColumnIndex("rowSize");

                    List<long[]> ids = new ArrayList<>();
                    boolean done = false;
                    for (int i = 0; i < 1000 && !done; i++) {
                        int i1 = 0;
                        long[] idArray = new long[100];
                        for (; i1 < 100 && !done; i1++) {
                            done = !(currentSize > maxSize && cursor.moveToNext());
                            if (!done) {
                                idArray[i1] = cursor.getLong(idIndex);
                                currentSize -= cursor.getLong(rowSizeIndex);
                            }
                        }
                        if (i1 == 100) {
                            ids.add(idArray);
                        } else {
                            ids.add(Arrays.copyOf(idArray, i1));
                        }
                    }
                    cursor.close();
                    SQLiteStatement deleteQuery = null;
                    int lastLength = 0;
                    for (long[] idArray : ids) {
                        if (deleteQuery == null || lastLength != idArray.length) {
                            deleteQuery = buildDeleteQuery(writableDatabase, idArray.length);
                            lastLength = idArray.length;
                        }
                        executeDeleteQuery(deleteQuery, idArray, idArray.length);
                    }
                }
            } finally {
                if (cursor != null && !cursor.isClosed()) {
                    cursor.close();
                }
            }
        }
    }

    private static SQLiteStatement buildDeleteQuery(SQLiteDatabase database, int numIds) {
        StringBuilder queryBuilder = new StringBuilder("DELETE FROM ").append(DATA_PACKET_QUEUE_TABLE_NAME).append(" WHERE ").append(ID_COLUMN).append(" IN (");
        for (int i = 0; i < numIds; i++) {
            queryBuilder.append("?, ");
        }
        queryBuilder.setLength(queryBuilder.length() - 2);
        return database.compileStatement(queryBuilder.append(")").toString());
    }

    private static int executeDeleteQuery(SQLiteStatement sqLiteStatement, long[] ids, int numIds) {
        sqLiteStatement.clearBindings();
        for (int i = 0; i < numIds; i++) {
            sqLiteStatement.bindLong(i, ids[i]);
        }
        return sqLiteStatement.executeUpdateDelete();
    }

    @Override
    public void enqueue(Iterator<DataPacket> dataPackets) throws IOException {
        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        writableDatabase.beginTransaction();
        try {
            while (dataPackets.hasNext()) {
                DataPacket dataPacket = dataPackets.next();
                ContentValues contentValues = new ContentValues();
                JSONObject attributesObject = new JSONObject();
                for (Map.Entry<String, String> entry : dataPacket.getAttributes().entrySet()) {
                    try {
                        attributesObject.put(entry.getKey(), entry.getValue());
                    } catch (JSONException e) {
                        throw new IOException("Unable to put attribute value of \"" + entry.getValue() + "\" for key \"" + entry.getKey() + "\"");
                    }
                }
                long createdTime = new Date().getTime();
                contentValues.put(CREATED_COLUMN, createdTime);
                contentValues.put(DATA_PACKET_QEUE_PRIORITY_COLUMN, dataPacketPrioritizer.getPriority(dataPacket));
                contentValues.put(DATA_PACKET_QUEUE_ATTRIBUTES_COLUMN, attributesObject.toString().getBytes(Charsets.UTF_8));
                InputStream inputStream = dataPacket.getData();
                try {
                    contentValues.put(CONTENT_COLUMN, IOUtils.readInputStream(inputStream));
                } finally {
                    inputStream.close();
                }
                long ttl = dataPacketPrioritizer.getTtl(dataPacket);
                if (ttl < 0) {
                    contentValues.put(DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN, Long.MAX_VALUE);
                } else {
                    contentValues.put(DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN, createdTime + ttl);
                }
                writableDatabase.insert(DATA_PACKET_QUEUE_TABLE_NAME, null, contentValues);
            }
            writableDatabase.setTransactionSuccessful();
        } finally {
            writableDatabase.endTransaction();
            writableDatabase.close();
        }
    }

    private void truncateToLimits(SQLiteDatabase writableDatabase, long maxRows, long maxSize) {
        writableDatabase.execSQL("DELETE FROM " + DATA_PACKET_QUEUE_TABLE_NAME + " WHERE " + DATA_PACKET_QUEUE_EXPIRATION_MILLIS_COLUMN + " <= ?", new Object[]{new Date().getTime()});
        ageOffRowCount(writableDatabase, maxRows);
        ageOffSize(writableDatabase, maxSize);
    }

    @Override
    public void process() throws IOException {
        while (doProcess()) {
            Log.d(CANONICAL_NAME, " processed batch of transactions");
        }
    }

    public boolean doProcess() throws IOException {
        String transactionId = UUID.randomUUID().toString();
        SqlLiteDataPacketIterator sqlLiteDataPacketIterator = new SqlLiteDataPacketIterator(siteToSiteDB, transactionId, limit);
        if (!sqlLiteDataPacketIterator.hasNext()) {
            return false;
        }
        TransactionResult transactionResult = null;
        Transaction transaction = siteToSiteClient.createTransaction();
        try {
            while (sqlLiteDataPacketIterator.hasNext()) {
                transaction.send(sqlLiteDataPacketIterator.next());
            }
            transaction.confirm();
            transactionResult = transaction.complete();
        } catch (Exception e) {
            sqlLiteDataPacketIterator.transactionFailed();
        }
        if (transactionResult != null) {
            sqlLiteDataPacketIterator.transactionComplete();
            return true;
        }
        return false;
    }
}
