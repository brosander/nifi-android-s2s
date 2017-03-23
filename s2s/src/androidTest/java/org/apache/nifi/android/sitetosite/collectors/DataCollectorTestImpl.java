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
import android.os.Parcelable;

import org.apache.nifi.android.sitetosite.packet.DataPacket;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DataCollectorTestImpl implements DataCollector {
    private final Queue<List<DataPacket>> dataPacketListQueue = new ConcurrentLinkedQueue<>();

    public static final Creator<DataCollectorTestImpl> CREATOR = new Creator<DataCollectorTestImpl>() {
        @Override
        public DataCollectorTestImpl createFromParcel(Parcel source) {
            DataCollectorTestImpl dataCollectorTest = new DataCollectorTestImpl();
            int numLists = source.readInt();
            for (int i = 0; i < numLists; i++) {
                Parcelable[] parcelables = source.readParcelableArray(DataCollectorTestImpl.class.getClassLoader());
                List<DataPacket> dataPackets = new ArrayList<>(parcelables.length);
                for (Parcelable parcelable : parcelables) {
                    dataPackets.add((DataPacket)parcelable);
                }
                dataCollectorTest.dataPacketListQueue.offer(dataPackets);
            }
            return dataCollectorTest;
        }

        @Override
        public DataCollectorTestImpl[] newArray(int size) {
            return new DataCollectorTestImpl[size];
        }
    };

    public DataCollectorTestImpl() {
    }

    public DataCollectorTestImpl(List<? extends DataPacket>... dataPacketLists) {
        for (List<? extends DataPacket> dataPackets: dataPacketLists) {
            dataPacketListQueue.offer(new ArrayList<>(dataPackets));
        }
    }

    @Override
    public Iterable<DataPacket> getDataPackets() {
        List<DataPacket> dataPackets = dataPacketListQueue.poll();
        if (dataPackets == null) {
            return Collections.emptyList();
        }
        return dataPackets;
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeInt(dataPacketListQueue.size());
        for (List<DataPacket> dataPackets : dataPacketListQueue) {
            dest.writeParcelableArray(dataPackets.toArray(new DataPacket[dataPackets.size()]), 0);
        }
    }
}
