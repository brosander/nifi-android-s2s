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

package org.apache.nifi.android.sitetosite.util;

import android.content.Intent;
import android.os.BadParcelableException;
import android.os.Parcel;
import android.os.Parcelable;

import java.io.Serializable;

public class IntentUtils {
    public static <T extends Parcelable> void putParcelable(T parcelable, Intent intent, String name) {
        byte[] bytes = marshallParcelable(parcelable);
        if (bytes == null) {
            return;
        }

        intent.putExtra(name + "_CLASSNAME", parcelable.getClass().getCanonicalName());
        intent.putExtra(name, bytes);
    }

    public static <T extends Parcelable> byte[] marshallParcelable(T parcelable) {
        if (parcelable == null) {
            return null;
        }
        Parcel parcel = Parcel.obtain();
        parcelable.writeToParcel(parcel, 0);
        parcel.setDataPosition(0);
        return parcel.marshall();
    }

    public static <T extends Serializable> byte[] marshallSerializable(T serializable) {
        if (serializable == null) {
            return null;
        }
        Parcel parcel = Parcel.obtain();
        parcel.writeSerializable(serializable);
        parcel.setDataPosition(0);
        return parcel.marshall();
    }

    public static <T extends Parcelable> T unmarshallParcelable(byte[] bytes, Class<T> clazz) {
        if (bytes == null) {
            return null;
        }
        Parcel parcel = Parcel.obtain();
        parcel.unmarshall(bytes, 0, bytes.length);
        parcel.setDataPosition(0);

        try {
            Parcelable.Creator<? extends Parcelable> creator = (Parcelable.Creator<? extends Parcelable>) clazz.getField("CREATOR").get(null);
            return (T) creator.createFromParcel(parcel);
        } catch (Exception e) {
            throw new BadParcelableException(e);
        }
    }

    public static <T extends Serializable> T unmarshallSerializable(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        Parcel parcel = Parcel.obtain();
        parcel.unmarshall(bytes, 0, bytes.length);
        parcel.setDataPosition(0);

        try {
            return (T) parcel.readSerializable();
        } catch (Exception e) {
            throw new BadParcelableException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Parcelable> T getParcelable(Intent intent, String name) {
        byte[] bytes = intent.getByteArrayExtra(name);
        if (bytes == null) {
            return null;
        }
        try {
            ClassLoader intentClassLoader = intent.getExtras().getClassLoader();
            String classname = intent.getStringExtra(name + "_CLASSNAME");
            Class<T> parcelable;
            if (intentClassLoader == null) {
                parcelable = (Class<T>) Class.forName(classname);
            } else {
                parcelable = (Class<T>) Class.forName(classname, true, intentClassLoader);
            }
            return unmarshallParcelable(bytes, parcelable);
        } catch (Exception e) {
            throw new BadParcelableException(e);
        }
    }
}
