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

package org.apache.nifi.android.sitetosite.collectors.filters;

import android.os.Parcel;

import java.io.File;
import java.io.FileFilter;

/**
 * Filter that accepts files modified within the window
 */
public class LastModifiedFileFilter implements ParcelableFileFilter {
    private final long minLastModified;
    private final long maxLastModified;

    public static final Creator<LastModifiedFileFilter> CREATOR = new Creator<LastModifiedFileFilter>() {
        @Override
        public LastModifiedFileFilter createFromParcel(Parcel source) {
            return new LastModifiedFileFilter(source.readLong(), source.readLong());
        }

        @Override
        public LastModifiedFileFilter[] newArray(int size) {
            return new LastModifiedFileFilter[size];
        }
    };

    public LastModifiedFileFilter(long minLastModified, long maxLastModified) {
        this.minLastModified = minLastModified;
        this.maxLastModified = maxLastModified;
    }

    @Override
    public boolean accept(File pathname) {
        long lastModified = pathname.lastModified();
        if (lastModified >= minLastModified && lastModified <= maxLastModified) {
            return true;
        }
        return false;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeLong(minLastModified);
        dest.writeLong(maxLastModified);
    }
}
