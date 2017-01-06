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

public class ParcelableFileFilterUtil {
    public static void writeToParcel(ParcelableFileFilter[] parcelableFileFilters, Parcel dest, int flags) {
        dest.writeInt(parcelableFileFilters.length);
        for (ParcelableFileFilter parcelableFileFilter : parcelableFileFilters) {
            dest.writeParcelable(parcelableFileFilter, flags);
        }
    }

    public static ParcelableFileFilter[] readFiltersFromParcel(Parcel source) {
        int len = source.readInt();
        ParcelableFileFilter[] parcelableFileFilters = new ParcelableFileFilter[len];
        for (int i = 0; i < len; i++) {
            parcelableFileFilters[i] = source.readParcelable(ParcelableFileFilterUtil.class.getClassLoader());
        }
        return parcelableFileFilters;
    }
}
