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

import android.database.sqlite.SQLiteDatabase;
import android.support.test.InstrumentationRegistry;

import org.apache.nifi.android.sitetosite.client.SiteToSiteClient;
import org.apache.nifi.android.sitetosite.client.Transaction;
import org.apache.nifi.android.sitetosite.client.TransactionResult;
import org.apache.nifi.android.sitetosite.client.persistence.SiteToSiteDB;
import org.apache.nifi.android.sitetosite.client.protocol.ResponseCode;
import org.apache.nifi.android.sitetosite.client.queue.DataPacketPrioritizer;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;
import org.apache.nifi.android.sitetosite.util.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SQLiteDataPacketQueueTest {
    public static final String TEST_PRIORITY = "test.priority";
    public static final String TEST_TTL = "test.ttl";
    public static final int MAX_SIZE = 1024 * 8;
    public static final int MAX_ROWS = 250;
    public static final int ITERATOR_SIZE_LIMIT = 10;
    public static final String ID = "id";

    private SiteToSiteDB siteToSiteDB;
    private SQLiteDataPacketQueue sqLiteDataPacketQueue;
    private String testTransactionId;
    private TestSiteToSiteClient siteToSiteClient;

    @Before
    public void setup() {
        siteToSiteDB = new SiteToSiteDB(InstrumentationRegistry.getContext());
        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            writableDatabase.delete(SiteToSiteDB.DATA_PACKET_QUEUE_TABLE_NAME, null, null);
        } finally {
            writableDatabase.close();
        }
        siteToSiteClient = new TestSiteToSiteClient();
        sqLiteDataPacketQueue = new SQLiteDataPacketQueue(siteToSiteClient, siteToSiteDB, new TestDataPacketPrioritizer(), MAX_ROWS, MAX_SIZE, ITERATOR_SIZE_LIMIT);
        testTransactionId = "testTransactionId";
    }

    @Test
    public void testNoEntries() {
        assertFalse(sqLiteDataPacketQueue.getSqLiteDataPacketIterator(testTransactionId).hasNext());
    }

    @Test
    public void testSingleInsertNullPriorityNoTtl() throws IOException {
        ByteArrayDataPacket byteArrayDataPacket = new ByteArrayDataPacket(Collections.singletonMap("id", "testId"), "testPayload".getBytes(Charsets.UTF_8));
        sqLiteDataPacketQueue.enqueue(byteArrayDataPacket);
        assertDataPacketsMatchIterator(Collections.singletonList(byteArrayDataPacket));
    }

    @Test
    public void testMultipleIteratorsInsertNullPriorityNoTtlWithFailures() throws IOException {
        List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < 155; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        Collections.reverse(dataPackets);

        for (int fromIndex = 0; fromIndex < dataPackets.size(); fromIndex += ITERATOR_SIZE_LIMIT) {
            List<DataPacket> expected = dataPackets.subList(fromIndex, Math.min(dataPackets.size(), fromIndex + ITERATOR_SIZE_LIMIT));

            SQLiteDataPacketIterator sqLiteDataPacketIterator = sqLiteDataPacketQueue.getSqLiteDataPacketIterator(testTransactionId);
            assertDataPacketsEqual(expected, sqLiteDataPacketIterator);
            sqLiteDataPacketIterator.transactionFailed();

            sqLiteDataPacketIterator = sqLiteDataPacketQueue.getSqLiteDataPacketIterator(testTransactionId);
            assertDataPacketsEqual(expected, sqLiteDataPacketIterator);
            sqLiteDataPacketIterator.transactionComplete();
        }
    }

    @Test
    public void testAgeOffRowCount() throws IOException {
        List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < 295; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        Collections.reverse(dataPackets);
        dataPackets = dataPackets.subList(0, MAX_ROWS);

        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            sqLiteDataPacketQueue.ageOffRowCount(writableDatabase);
        } finally {
            writableDatabase.close();
        }

        assertDataPacketsMatchIterator(dataPackets);
    }

    @Test
    public void testAgeOffRowCountPriority() throws IOException {
        List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < 295; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_PRIORITY, Integer.toString(1000 - i));
            attributes.put("id", "testId" + i);
            dataPackets.add(new ByteArrayDataPacket(attributes, ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        dataPackets = dataPackets.subList(0, MAX_ROWS);

        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            sqLiteDataPacketQueue.ageOffRowCount(writableDatabase);
        } finally {
            writableDatabase.close();
        }

        assertDataPacketsMatchIterator(dataPackets);
    }

    @Test
    public void testSizeAgeOff() throws IOException {
        int numPackets = 110000;
        List<DataPacket> dataPackets = new ArrayList<>(numPackets);
        for (int i = 0; i < numPackets; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        Collections.reverse(dataPackets);

        List<DataPacket> expectedDataPackets = new ArrayList<>();
        long size = 0L;
        for (DataPacket dataPacket : dataPackets) {
            size += sqLiteDataPacketQueue.getAttributesBytes(dataPacket).length;
            size += dataPacket.getSize();
            if (size <= MAX_SIZE) {
                expectedDataPackets.add(dataPacket);
            }
        }

        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        writableDatabase.beginTransaction();
        try {
            sqLiteDataPacketQueue.ageOffSize(writableDatabase);
            writableDatabase.setTransactionSuccessful();
        } finally {
            writableDatabase.endTransaction();
            writableDatabase.close();
        }

        assertDataPacketsMatchIterator(expectedDataPackets);
    }

    @Test
    public void testSizeAgeOffPriority() throws IOException {
        int numPackets = 110000;
        List<DataPacket> dataPackets = new ArrayList<>(numPackets);
        for (int i = 0; i < numPackets; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_PRIORITY, Integer.toString(numPackets - i));
            attributes.put("id", "testId" + i);
            dataPackets.add(new ByteArrayDataPacket(attributes, ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());

        List<DataPacket> expectedDataPackets = new ArrayList<>();
        long size = 0L;
        for (DataPacket dataPacket : dataPackets) {
            size += sqLiteDataPacketQueue.getAttributesBytes(dataPacket).length;
            size += dataPacket.getSize();
            if (size <= MAX_SIZE) {
                expectedDataPackets.add(dataPacket);
            }
        }

        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        writableDatabase.beginTransaction();
        try {
            sqLiteDataPacketQueue.ageOffSize(writableDatabase);
            writableDatabase.setTransactionSuccessful();
        } finally {
            writableDatabase.endTransaction();
            writableDatabase.close();
        }

        assertDataPacketsMatchIterator(expectedDataPackets);
    }

    @Test
    public void testTtlAgeOff() throws IOException, InterruptedException {
        List<DataPacket> dataPackets = new ArrayList<>();
        for (int i = 0; i < 155; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_TTL, "1");
            attributes.put("id", "testExpiredId" + i);
            dataPackets.add(new ByteArrayDataPacket(attributes, ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        long timeAfterInsert = System.currentTimeMillis();

        dataPackets = new ArrayList<>();
        for (int i = 0; i < 155; i++) {
            Map<String, String> attributes = new HashMap<>();
            attributes.put(TEST_TTL, "1000000");
            attributes.put(ID, "testNotExpiredId" + i);
            dataPackets.add(new ByteArrayDataPacket(attributes, ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        Collections.reverse(dataPackets);

        Thread.sleep(Math.max(0, (timeAfterInsert + 5) - System.currentTimeMillis()));

        SQLiteDatabase writableDatabase = siteToSiteDB.getWritableDatabase();
        try {
            sqLiteDataPacketQueue.ageOffTtl(writableDatabase);
        } finally {
            writableDatabase.close();
        }

        assertDataPacketsMatchIterator(dataPackets);
    }

    @Test
    public void testProcess() throws IOException {
        int numPackets = 155;
        List<DataPacket> dataPackets = new ArrayList<>(numPackets);
        for (int i = 0; i < numPackets; i++) {
            dataPackets.add(new ByteArrayDataPacket(Collections.singletonMap("id", "testId" + i), ("testPayload" + i).getBytes(Charsets.UTF_8)));
        }
        sqLiteDataPacketQueue.enqueue(dataPackets.iterator());
        sqLiteDataPacketQueue.process();
        Collections.reverse(dataPackets);

        int index = 0;
        for (TestTransaction testTransaction : siteToSiteClient.testTransactions) {
            for (String sentId : testTransaction.sentIds) {
                assertEquals(dataPackets.get(index++).getAttributes().get(ID), sentId);
            }
            assertTrue(testTransaction.confirmed);
            assertTrue(testTransaction.completed);
        }
    }

    private void assertDataPacketsMatchIterator(List<? extends DataPacket> expected) throws IOException {
        for (int fromIndex = 0; fromIndex < expected.size(); fromIndex += ITERATOR_SIZE_LIMIT) {
            SQLiteDataPacketIterator sqLiteDataPacketIterator = sqLiteDataPacketQueue.getSqLiteDataPacketIterator(testTransactionId);
            assertDataPacketsEqual(expected.subList(fromIndex, Math.min(expected.size(), fromIndex + ITERATOR_SIZE_LIMIT)), sqLiteDataPacketIterator);
            sqLiteDataPacketIterator.transactionComplete();
        }
    }

    private void assertDataPacketsEqual(List<? extends DataPacket> expected, SQLiteDataPacketIterator actual) throws IOException {
        for (DataPacket dataPacket : expected) {
            assertTrue(actual.hasNext());
            assertDataPacketsEqual(dataPacket, actual.next());
        }
        assertFalse(actual.hasNext());
    }

    private void assertDataPacketsEqual(DataPacket expected, DataPacket actual) throws IOException {
        assertEquals(expected.getAttributes(), actual.getAttributes());
        assertArrayEquals(IOUtils.readInputStream(expected.getData()), IOUtils.readInputStream(actual.getData()));
    }

    private class TestSiteToSiteClient implements SiteToSiteClient {
        private final List<TestTransaction> testTransactions = new ArrayList<>();

        @Override
        public Transaction createTransaction() throws IOException {
            TestTransaction testTransaction = new TestTransaction();
            testTransactions.add(testTransaction);
            return testTransaction;
        }
    }

    private class TestTransaction implements Transaction {
        private final List<String> sentIds = new ArrayList<>();
        private boolean confirmed = false;
        private boolean completed = false;
        private boolean cancelled = false;

        @Override
        public void send(DataPacket dataPacket) throws IOException {
            sentIds.add(dataPacket.getAttributes().get(ID));
        }

        @Override
        public void confirm() throws IOException {
            confirmed = true;
        }

        @Override
        public TransactionResult complete() throws IOException {
            completed = true;
            return new TransactionResult(sentIds.size(), ResponseCode.TRANSACTION_FINISHED, "");
        }

        @Override
        public TransactionResult cancel() throws IOException {
            cancelled = true;
            return new TransactionResult(sentIds.size(), ResponseCode.TRANSACTION_FINISHED, "");
        }
    }

    private class TestDataPacketPrioritizer implements DataPacketPrioritizer {

        @Override
        public long getPriority(DataPacket dataPacket) {
            String priorityString = dataPacket.getAttributes().get(TEST_PRIORITY);
            return priorityString == null ? 0L : Long.parseLong(priorityString);
        }

        @Override
        public long getTtl(DataPacket dataPacket) {
            String ttlString = dataPacket.getAttributes().get(TEST_TTL);
            return ttlString == null ? -1L : Long.parseLong(ttlString);
        }
    }
}
