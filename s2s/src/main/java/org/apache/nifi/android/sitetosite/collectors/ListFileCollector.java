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

package org.apache.nifi.android.sitetosite.collectors;

import android.os.Parcel;

import org.apache.nifi.android.sitetosite.collectors.filters.AndFileFilter;
import org.apache.nifi.android.sitetosite.collectors.filters.DirectoryFileFilter;
import org.apache.nifi.android.sitetosite.collectors.filters.LastModifiedFileFilter;
import org.apache.nifi.android.sitetosite.collectors.filters.OrFileFilter;
import org.apache.nifi.android.sitetosite.collectors.filters.ParcelableFileFilter;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.packet.FileDataPacket;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.List;

/**
 * DataCollector that lists files in a directory according to a file filter and optionally modified time
 */
public class ListFileCollector implements DataCollector {
    private final File baseDir;
    private final ParcelableFileFilter fileFilter;
    private boolean filterModified;
    private long minModifiedTime;

    public static final Creator<ListFileCollector> CREATOR = new Creator<ListFileCollector>() {
        @Override
        public ListFileCollector createFromParcel(Parcel source) {
            return new ListFileCollector(new File(source.readString()),
                    source.<ParcelableFileFilter>readParcelable(ListFileCollector.class.getClassLoader()), Boolean.valueOf(source.readString()), source.readLong());
        }

        @Override
        public ListFileCollector[] newArray(int size) {
            return new ListFileCollector[size];
        }
    };

    public ListFileCollector(File baseDir, ParcelableFileFilter fileFilter) {
        this(baseDir, fileFilter, false, 0L);
    }

    public ListFileCollector(File baseDir, ParcelableFileFilter fileFilter, long minModifiedTime) {
        this(baseDir, fileFilter, true, minModifiedTime);
    }

    protected ListFileCollector(File baseDir, ParcelableFileFilter fileFilter, boolean filterModified, long minModifiedTime) {
        this.baseDir = baseDir;
        this.fileFilter = fileFilter;
        this.filterModified = filterModified;
        this.minModifiedTime = minModifiedTime;
    }

    @Override
    public Iterable<DataPacket> getDataPackets() {
        long maxLastModified = System.currentTimeMillis() - 1;
        List<DataPacket> dataPackets = new ArrayList<>();
        FileFilter fileFilter;
        if (filterModified) {
            // Filter out any files not modified in window
            ParcelableFileFilter modifiedCompoundFilter = new OrFileFilter(new DirectoryFileFilter(), new LastModifiedFileFilter(minModifiedTime, maxLastModified));
            fileFilter = new AndFileFilter(modifiedCompoundFilter, this.fileFilter);
        } else {
            fileFilter = this.fileFilter;
        }
        listRecursive(baseDir, fileFilter, dataPackets);
        minModifiedTime = maxLastModified + 1;
        return dataPackets;
    }

    private void listRecursive(File base, FileFilter fileFilter, List<DataPacket> output) {
        for (final File file : base.listFiles(fileFilter)) {
            if (file.isFile()) {
                output.add(new FileDataPacket(file));
            } else if (file.isDirectory()) {
                listRecursive(file, fileFilter, output);
            }
        }
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(baseDir.getAbsolutePath());
        dest.writeParcelable(fileFilter, flags);
        dest.writeString(Boolean.toString(filterModified));
        dest.writeLong(minModifiedTime);
    }
}
