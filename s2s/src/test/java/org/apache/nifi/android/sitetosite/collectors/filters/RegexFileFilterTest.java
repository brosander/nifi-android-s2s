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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.File;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RegexFileFilterTest {
    @Mock
    private File file;

    private String regex;

    private boolean matchAbsolutePath;

    private RegexFileFilter regexFileFilter;

    @Before
    public void setup() {
        regex = "test.*\\.java";
        matchAbsolutePath = false;
        regexFileFilter = new RegexFileFilter(regex, matchAbsolutePath);
    }

    @Test
    public void testRegexMatchJustName() {
        when(file.getName()).thenReturn("testMe.java");
        assertTrue(regexFileFilter.accept(file));
    }

    @Test
    public void testRegexNoMatchJustName() {
        when(file.getName()).thenReturn("testMe.jar");
        assertFalse(regexFileFilter.accept(file));
    }

    @Test
    public void testRegexMatchAbsolutePath() {
        regex = ".*test.*\\.java";
        matchAbsolutePath = true;
        regexFileFilter = new RegexFileFilter(regex, matchAbsolutePath);
        when(file.getAbsolutePath()).thenReturn("/abs/path/to/testMe.java");
        assertTrue(regexFileFilter.accept(file));
    }

    @Test
    public void testRegexNoMatchAbsolutePath() {
        matchAbsolutePath = true;
        regexFileFilter = new RegexFileFilter(regex, matchAbsolutePath);
        when(file.getAbsolutePath()).thenReturn("/abs/path/to/testMe.java");
        assertFalse(regexFileFilter.accept(file));
    }
}
