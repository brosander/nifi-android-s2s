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

package com.hortonworks.hdf.android.sitetosite.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class MockScheduledExecutor extends ScheduledThreadPoolExecutor {
    private final List<ScheduledWithFixedDelayInvocation> scheduledWithFixedDelayInvocations = new ArrayList<>();

    public MockScheduledExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        scheduledWithFixedDelayInvocations.add(new ScheduledWithFixedDelayInvocation(command, initialDelay, delay, unit));
        return new ScheduledFuture<Object>() {
            final AtomicBoolean cancelled = new AtomicBoolean(false);

            @Override
            public long getDelay(TimeUnit unit) {
                return 0;
            }

            @Override
            public int compareTo(Delayed o) {
                return 0;
            }

            @Override
            public boolean cancel(boolean mayInterruptIfRunning) {
                cancelled.set(true);
                return true;
            }

            @Override
            public boolean isCancelled() {
                return cancelled.get();
            }

            @Override
            public boolean isDone() {
                return false;
            }

            @Override
            public Object get() throws InterruptedException, ExecutionException {
                return null;
            }

            @Override
            public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
                return null;
            }
        };
    }

    public List<ScheduledWithFixedDelayInvocation> getScheduledWithFixedDelayInvocations() {
        return scheduledWithFixedDelayInvocations;
    }

    public Runnable getTtlExtender(long expectedTimeInSeconds) {
        assertEquals(1, scheduledWithFixedDelayInvocations.size());
        MockScheduledExecutor.ScheduledWithFixedDelayInvocation scheduledWithFixedDelayInvocation = scheduledWithFixedDelayInvocations.get(0);
        TimeUnit timeUnit = scheduledWithFixedDelayInvocation.getTimeUnit();
        assertEquals(expectedTimeInSeconds, timeUnit.toSeconds(scheduledWithFixedDelayInvocation.getInitialDelay()));
        assertEquals(expectedTimeInSeconds, timeUnit.toSeconds(scheduledWithFixedDelayInvocation.getDelay()));
        return scheduledWithFixedDelayInvocation.getRunnable();
    }

    public static class ScheduledWithFixedDelayInvocation {
        private final Runnable runnable;
        private final long initialDelay;
        private final long delay;
        private final TimeUnit timeUnit;

        public ScheduledWithFixedDelayInvocation(Runnable runnable, long initialDelay, long delay, TimeUnit timeUnit) {

            this.runnable = runnable;
            this.initialDelay = initialDelay;
            this.delay = delay;
            this.timeUnit = timeUnit;
        }

        public Runnable getRunnable() {
            return runnable;
        }

        public long getInitialDelay() {
            return initialDelay;
        }

        public long getDelay() {
            return delay;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }
    }
}
