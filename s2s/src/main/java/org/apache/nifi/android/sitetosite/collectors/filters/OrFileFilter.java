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
 * Filter that accepts a file iff at least one of its delegates do
 */
public class OrFileFilter implements ParcelableFileFilter {
    private final ParcelableFileFilter[] delegates;

    public static final Creator<OrFileFilter> CREATOR = new Creator<OrFileFilter>() {
        @Override
        public OrFileFilter createFromParcel(Parcel source) {
            return new OrFileFilter(ParcelableFileFilterUtil.readFiltersFromParcel(source));
        }

        @Override
        public OrFileFilter[] newArray(int size) {
            return new OrFileFilter[size];
        }
    };

    public OrFileFilter(ParcelableFileFilter... delegates) {
        this.delegates = delegates;
    }

    @Override
    public boolean accept(File pathname) {
        for (FileFilter delegate : delegates) {
            if (delegate.accept(pathname)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        ParcelableFileFilterUtil.writeToParcel(delegates, dest, flags);
    }
}
