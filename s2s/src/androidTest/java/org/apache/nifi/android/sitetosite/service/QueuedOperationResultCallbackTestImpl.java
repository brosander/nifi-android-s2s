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

import org.apache.nifi.android.sitetosite.client.QueuedSiteToSiteClientConfig;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

public class QueuedOperationResultCallbackTestImpl implements QueuedOperationResultCallback {
    private final Handler handler;
    private final AtomicReference<QueuedSiteToSiteClientConfig> queuedSiteToSiteClientConfigAtomicReference = new AtomicReference<>();
    private final AtomicReference<IOException> ioExceptionAtomicReference = new AtomicReference<>();
    private final CountDownLatch countDownLatch = new CountDownLatch(1);

    public QueuedOperationResultCallbackTestImpl() {
        this(null);
    }

    public QueuedOperationResultCallbackTestImpl(Handler handler) {
        this.handler = handler;
    }

    @Override
    public Handler getHandler() {
        return handler;
    }

    @Override
    public void onSuccess(QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig) {
        assertNull(queuedSiteToSiteClientConfigAtomicReference.getAndSet(queuedSiteToSiteClientConfig));
        countDownLatch.countDown();
    }

    @Override
    public void onException(IOException exception, QueuedSiteToSiteClientConfig queuedSiteToSiteClientConfig) {
        assertNull(ioExceptionAtomicReference.getAndSet(exception));
        onSuccess(queuedSiteToSiteClientConfig);
    }

    public QueuedSiteToSiteClientConfig getQueuedSiteToSiteClientConfig() throws InterruptedException {
        countDownLatch.await();
        return queuedSiteToSiteClientConfigAtomicReference.get();
    }

    public IOException getIOException() throws InterruptedException {
        countDownLatch.await();
        return ioExceptionAtomicReference.get();
    }
}
