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
 * Filter that accepts all directories
 */
public class DirectoryFileFilter implements ParcelableFileFilter {
    public static final Creator<DirectoryFileFilter> CREATOR = new Creator<DirectoryFileFilter>() {
        @Override
        public DirectoryFileFilter createFromParcel(Parcel source) {
            return new DirectoryFileFilter();
        }

        @Override
        public DirectoryFileFilter[] newArray(int size) {
            return new DirectoryFileFilter[size];
        }
    };

    @Override
    public boolean accept(File pathname) {
        return pathname.isDirectory();
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {

    }
}
