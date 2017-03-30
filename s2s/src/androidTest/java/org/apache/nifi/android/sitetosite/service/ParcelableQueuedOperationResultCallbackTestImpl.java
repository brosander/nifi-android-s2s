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

import android.content.Context;
import android.os.Parcel;
import android.os.Parcelable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class ParcelableQueuedOperationResultCallbackTestImpl implements ParcelableQueuedOperationResultCallback {
    private static final Map<String, Queue<Invocation>> invocations = new ConcurrentHashMap<>();
    private static final Map<String, CountDownLatch> countDownLatches = new ConcurrentHashMap<>();

    public static Creator<ParcelableQueuedOperationResultCallbackTestImpl> CREATOR = new Creator<ParcelableQueuedOperationResultCallbackTestImpl>() {
        @Override
        public ParcelableQueuedOperationResultCallbackTestImpl createFromParcel(Parcel source) {
            return new ParcelableQueuedOperationResultCallbackTestImpl(source.readString());
        }

        @Override
        public ParcelableQueuedOperationResultCallbackTestImpl[] newArray(int size) {
            return new ParcelableQueuedOperationResultCallbackTestImpl[size];
        }
    };

    private final String uuid;

    public ParcelableQueuedOperationResultCallbackTestImpl() {
        this(1);
    }

    public ParcelableQueuedOperationResultCallbackTestImpl(int count) {
        this(UUID.randomUUID().toString());
        invocations.put(uuid, new ConcurrentLinkedQueue<Invocation>());
        countDownLatches.put(uuid, new CountDownLatch(count));
    }

    private ParcelableQueuedOperationResultCallbackTestImpl(String uuid) {
        this.uuid = uuid;
    }

    @Override
    public void onSuccess(Context context) {
        invocations.get(uuid).offer(new Invocation(Type.SUCCESS, null));
        countDownLatches.get(uuid).countDown();
    }

    @Override
    public void onException(Context context, IOException exception) {
        invocations.get(uuid).offer(new Invocation(Type.FAILURE, exception));
        countDownLatches.get(uuid).countDown();
    }

    public List<Invocation> getInvocations() throws InterruptedException {
        countDownLatches.get(uuid).await();
        countDownLatches.remove(uuid);
        return new ArrayList<>(invocations.remove(uuid));
    }

    public void reinitCountDown(int count) {
        countDownLatches.put(uuid, new CountDownLatch(count));
    }

    public void await() throws InterruptedException {
        countDownLatches.get(uuid).await();
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(uuid);
    }

    public enum Type {
        SUCCESS, FAILURE
    }

    public static class Invocation implements Parcelable {
        private final Type type;
        private final IOException ioException;

        public static final Creator<Invocation> CREATOR = new Creator<Invocation>() {
            @Override
            public Invocation createFromParcel(Parcel source) {
                return new Invocation(Type.valueOf(source.readString()), (IOException)source.readSerializable());
            }

            @Override
            public Invocation[] newArray(int size) {
                return new Invocation[size];
            }
        };

        public Invocation(Type type, IOException ioException) {
            this.type = type;
            this.ioException = ioException;
        }

        public Type getType() {
            return type;
        }

        public IOException getIoException() {
            return ioException;
        }

        @Override
        public int describeContents() {
            return 0;
        }

        @Override
        public void writeToParcel(Parcel dest, int flags) {
            dest.writeString(type.name());
            dest.writeSerializable(ioException);
        }
    }
}
