/*
 * Copyright 2017 Hortonworks, Inc.
 * All rights reserved.
 *
 *   Hortonworks, Inc. licenses this file to you under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License. You may obtain a copy of the License at
 *   http://www.apache.org/licenses/LICENSE-2.0
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * See the associated NOTICE file for additional information regarding copyright ownership.
 */

package com.hortonworks.hdf.android.sitetosite.service;

import android.os.Handler;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

public class QueuedOperationResultCallbackTestImpl implements QueuedOperationResultCallback {
    private final Handler handler;
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
    public void onSuccess() {
        countDownLatch.countDown();
    }

    @Override
    public void onException(IOException exception) {
        assertNull(ioExceptionAtomicReference.getAndSet(exception));
        countDownLatch.countDown();
    }

    public IOException getIOException() throws InterruptedException {
        countDownLatch.await();
        return ioExceptionAtomicReference.get();
    }
}
