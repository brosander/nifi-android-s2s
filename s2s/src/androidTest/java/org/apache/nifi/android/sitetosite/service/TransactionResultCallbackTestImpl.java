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

import android.os.Handler;

import org.apache.nifi.android.sitetosite.client.TransactionResult;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

public class TransactionResultCallbackTestImpl implements TransactionResultCallback {
    private final Handler handler;
    private final AtomicReference<TransactionResult> transactionResultAtomicReference = new AtomicReference<>();
    private final AtomicReference<IOException> ioExceptionAtomicReference = new AtomicReference<>();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public TransactionResultCallbackTestImpl() {
        this(null);
    }

    public TransactionResultCallbackTestImpl(Handler handler) {
        this.handler = handler;
    }

    @Override
    public Handler getHandler() {
        return handler;
    }

    @Override
    public void onSuccess(TransactionResult transactionResult) {
        assertNull(transactionResultAtomicReference.getAndSet(transactionResult));
        countDownLatch.countDown();
    }

    @Override
    public void onException(IOException exception) {
        assertNull(ioExceptionAtomicReference.getAndSet(exception));
        onSuccess(null);
    }

    public TransactionResult getTransactionResult() throws InterruptedException {
        countDownLatch.await();
        return transactionResultAtomicReference.get();
    }

    public IOException getIOException() throws InterruptedException {
        countDownLatch.await();
        return ioExceptionAtomicReference.get();
    }
}
