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

import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.util.SerializationUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.CREATED_COLUMN;
import static org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB.ID_COLUMN;

public class DemoAppDB {
    public static final String TRANSACTION_LOG_ENTRY_SAVED = DemoAppDB.class.getCanonicalName() + ".save(transactionLogEntry)";

    public static final String S2S_TABLE_NAME = "APACHE_NIFI_SITE_TO_SITE_LOG";
    public static final String S2S_TRANSACTION_RESULT_COLUMN = "TRANSACTION_RESULT";
    public static final String S2S_IO_EXCEPTION_COLUMN = "IO_EXCEPTION";

    public static final int VERSION = 1;

    private static SQLiteOpenHelper sqLiteOpenHelper;

    private final Context context;

    public DemoAppDB(Context context) {
        this.context = context;
        synchronized (DemoAppDB.class) {
            if (sqLiteOpenHelper == null) {
                sqLiteOpenHelper = new SQLiteOpenHelper(context, DemoAppDB.class.getSimpleName() + ".db", null, VERSION) {
                    @Override
                    public void onCreate(SQLiteDatabase db) {
                        db.execSQL("CREATE TABLE " + S2S_TABLE_NAME + " (" +
                                ID_COLUMN + " INTEGER PRIMARY KEY, " +
                                CREATED_COLUMN + " INTEGER, " +
                                S2S_TRANSACTION_RESULT_COLUMN + " BLOB, " +
                                S2S_IO_EXCEPTION_COLUMN + " BLOB)");
                    }

                    @Override
                    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {

                    }
                };
            }
        }
    }

    /**
     * Saves the transactionLogEntry
     *
     * @param transactionLogEntry the transactionLogEntry
     */
    public void save(TransactionLogEntry transactionLogEntry) {
        SQLiteDatabase writableDatabase = sqLiteOpenHelper.getWritableDatabase();
        try {
            ContentValues values = new ContentValues();
            values.put(CREATED_COLUMN, transactionLogEntry.getCreated().getTime());
            values.put(S2S_TRANSACTION_RESULT_COLUMN, SerializationUtils.marshallParcelable(transactionLogEntry.getTransactionResult()));
            values.put(S2S_IO_EXCEPTION_COLUMN, SerializationUtils.marshallSerializable(transactionLogEntry.getIoException()));
            transactionLogEntry.setId(writableDatabase.insert(S2S_TABLE_NAME, null, values));
            context.sendBroadcast(new Intent(TRANSACTION_LOG_ENTRY_SAVED));
        } finally {
            writableDatabase.close();
        }
    }

    /**
     * Gets all transactionLogEntries with a timestamp after the timestamp parameter
     *
     * @param lastTimestamp the timestamp
     * @return all transactionLogEntries after the timestamp
     */
    public List<TransactionLogEntry> getLogEntries(long lastTimestamp) {
        List<TransactionLogEntry> transactionLogEntries = new ArrayList<>();
        SQLiteDatabase readableDatabase = sqLiteOpenHelper.getReadableDatabase();
        try {
            Cursor cursor = readableDatabase.query(false, S2S_TABLE_NAME, new String[]{ID_COLUMN, CREATED_COLUMN, S2S_TRANSACTION_RESULT_COLUMN, S2S_IO_EXCEPTION_COLUMN}, CREATED_COLUMN + " > ?", new String[]{Long.toString(lastTimestamp)}, null, null, "CREATED", null);
            try {
                int idIndex = cursor.getColumnIndexOrThrow(ID_COLUMN);
                int createdIndex = cursor.getColumnIndexOrThrow(CREATED_COLUMN);
                int transactionResultIndex = cursor.getColumnIndexOrThrow(S2S_TRANSACTION_RESULT_COLUMN);
                int ioeIndex = cursor.getColumnIndexOrThrow(S2S_IO_EXCEPTION_COLUMN);
                while (cursor.moveToNext()) {
                    transactionLogEntries.add(new TransactionLogEntry(cursor.getLong(idIndex),
                            new Date(cursor.getLong(createdIndex)),
                            SerializationUtils.unmarshallParcelable(cursor.getBlob(transactionResultIndex), TransactionResult.class),
                            SerializationUtils.<IOException>unmarshallSerializable(cursor.getBlob(ioeIndex))));
                }
            } finally {
                cursor.close();
            }
        } finally {
            readableDatabase.close();
        }
        return transactionLogEntries;
    }
}
