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
public class LastModifiedFileFilterTest {
    @Mock
    private File file;

    private LastModifiedFileFilter lastModifiedFileFilter;

    @Before
    public void setup() {
        lastModifiedFileFilter = new LastModifiedFileFilter(100, 200);
    }

    @Test
    public void testDoesntAcceptBeforeMin() {
        when(file.lastModified()).thenReturn(99L);
        assertFalse(lastModifiedFileFilter.accept(file));
    }

    @Test
    public void testAcceptsMin() {
        when(file.lastModified()).thenReturn(100L);
        assertTrue(lastModifiedFileFilter.accept(file));
    }

    @Test
    public void testAcceptsMiddle() {
        when(file.lastModified()).thenReturn(150L);
        assertTrue(lastModifiedFileFilter.accept(file));
    }

    @Test
    public void testAcceptsMax() {
        when(file.lastModified()).thenReturn(200L);
        assertTrue(lastModifiedFileFilter.accept(file));
    }

    @Test
    public void testDoesntAcceptGreaterThanMax() {
        when(file.lastModified()).thenReturn(201L);
        assertFalse(lastModifiedFileFilter.accept(file));
    }
}
