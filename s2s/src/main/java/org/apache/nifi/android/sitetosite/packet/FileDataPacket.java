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

package org.apache.nifi.android.sitetosite.packet;

import android.os.Parcel;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Data packet for a file object
 */
public class FileDataPacket implements DataPacket {
    private final File file;

    public FileDataPacket(File file) {
        this.file = file;
    }

    public static final Creator<FileDataPacket> CREATOR = new Creator<FileDataPacket>() {
        @Override
        public FileDataPacket createFromParcel(Parcel in) {
            return new FileDataPacket(new File(in.readString()));
        }

        @Override
        public FileDataPacket[] newArray(int size) {
            return new FileDataPacket[size];
        }
    };

    @Override
    public Map<String, String> getAttributes() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("path", file.getParentFile().getPath());
        attributes.put("absolute.path", file.getParentFile().getAbsolutePath());
        attributes.put("filename", file.getName());
        return attributes;
    }

    @Override
    public InputStream getData() {
        try {
            return new FileInputStream(file);
        } catch (FileNotFoundException e) {
            throw new DataPacketGetDataException(e);
        }
    }

    @Override
    public long getSize() {
        return file.length();
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(file.getAbsolutePath());
    }
}
