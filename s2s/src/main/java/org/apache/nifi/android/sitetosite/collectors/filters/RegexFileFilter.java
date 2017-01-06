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
import java.util.regex.Pattern;

/**
 * File filter that only accepts files matching a regex
 */
public class RegexFileFilter implements ParcelableFileFilter {
    private final String regex;
    private final Pattern pattern;
    private final boolean matchAbsolutePath;

    public static final Creator<RegexFileFilter> CREATOR = new Creator<RegexFileFilter>() {
        @Override
        public RegexFileFilter createFromParcel(Parcel source) {
            return new RegexFileFilter(source.readString(), Boolean.valueOf(source.readString()));
        }

        @Override
        public RegexFileFilter[] newArray(int size) {
            return new RegexFileFilter[size];
        }
    };

    /**
     * Creates the RegexFileFilter
     *
     * @param regex             regex to match
     * @param matchAbsolutePath boolean indicating whether to match against absolute path or just file name
     */
    public RegexFileFilter(String regex, boolean matchAbsolutePath) {
        this.regex = regex;
        this.pattern = Pattern.compile(regex);
        this.matchAbsolutePath = matchAbsolutePath;
    }

    @Override
    public boolean accept(File pathname) {
        String pathToMatch = matchAbsolutePath ? pathname.getAbsolutePath() : pathname.getName();
        return pattern.matcher(pathToMatch).matches();
    }

    @Override
    public int describeContents() {
        return 0;
    }

    @Override
    public void writeToParcel(Parcel dest, int flags) {
        dest.writeString(regex);
        dest.writeString(Boolean.toString(matchAbsolutePath));
    }
}
