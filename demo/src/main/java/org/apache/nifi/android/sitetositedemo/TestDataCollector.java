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

package org.apache.nifi.android.sitetositedemo;

import android.os.Parcel;
import android.support.annotation.NonNull;

import org.apache.nifi.android.sitetosite.collectors.DataCollector;
import org.apache.nifi.android.sitetosite.packet.ByteArrayDataPacket;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.apache.nifi.android.sitetosite.util.Charsets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class TestDataCollector implements DataCollector {
    private final String message;
    private int num = 0;
    public static Creator<TestDataCollector> CREATOR = new Creator<TestDataCollector>() {
        @Override
        public TestDataCollector createFromParcel(Parcel source) {
            TestDataCollector result = new TestDataCollector(source.readString());
            result.num = source.readInt();
            return result;
        }

        @Override
        public TestDataCollector[] newArray(int size) {
            return new TestDataCollector[size];
        }
    };

    public TestDataCollector(String message) {
        this.message = message;
    }

    @Override
    public Iterable<DataPacket> getDataPackets() {
        return new ArrayList<>(Arrays.asList(getDataPacket()));
    }

    @NonNull
    private DataPacket getDataPacket() {
        Map<String, String> attributes = new HashMap<>();
        attributes.put("number", Integer.toString(num++));
        return new ByteArrayDataPacket(attributes, message.getBytes(Charsets.UTF_8));
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(message);
        dest.writeInt(num);
    }
}
