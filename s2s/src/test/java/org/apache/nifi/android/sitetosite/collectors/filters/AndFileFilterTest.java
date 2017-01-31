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
import java.io.FileFilter;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class AndFileFilterTest {
    @Mock
    private ParcelableFileFilter delegate1;

    @Mock
    private ParcelableFileFilter delegate2;

    @Mock
    private File file;

    private AndFileFilter andFileFilter;

    @Before
    public void setup() {
        andFileFilter = new AndFileFilter(delegate1, delegate2);
    }

    @Test
    public void testBothAccept() {
        when(delegate1.accept(file)).thenReturn(true);
        when(delegate2.accept(file)).thenReturn(true);
        assertTrue(andFileFilter.accept(file));
        verify(delegate1).accept(file);
        verify(delegate2).accept(file);
    }

    @Test
    public void testFirstDoesntAccept() {
        when(delegate1.accept(file)).thenReturn(false);
        assertFalse(andFileFilter.accept(file));
        verify(delegate1).accept(file);
        verify(delegate2, never()).accept(file);
    }

    @Test
    public void testSecondDoesntAccept() {
        when(delegate1.accept(file)).thenReturn(true);
        when(delegate2.accept(file)).thenReturn(false);
        assertFalse(andFileFilter.accept(file));
        verify(delegate1).accept(file);
        verify(delegate2).accept(file);
    }
}
