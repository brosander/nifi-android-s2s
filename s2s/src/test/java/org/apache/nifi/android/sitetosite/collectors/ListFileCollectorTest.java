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

import org.apache.nifi.android.sitetosite.collectors.filters.ParcelableFileFilter;
import org.apache.nifi.android.sitetosite.packet.DataPacket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.FileFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ListFileCollectorTest {
    @Mock
    private File file;

    @Mock
    private ParcelableFileFilter fileFilter;

    private ListFileCollector listFileCollector;

    @Before
    public void setup() {
        listFileCollector = new ListFileCollector(file, fileFilter);
    }

    @Test
    public void testFindsFileInDir() {
        File childFile = mock(File.class);
        when(file.listFiles(fileFilter)).thenReturn(new File[]{childFile});
        String childFileName = "childFile";
        when(childFile.getName()).thenReturn(childFileName);
        when(childFile.isFile()).thenReturn(true);
        when(childFile.getParentFile()).thenReturn(file);

        Iterable<DataPacket> dataPackets = listFileCollector.getDataPackets();
        Iterator<DataPacket> iterator = dataPackets.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(childFileName, iterator.next().getAttributes().get("filename"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testRecursive() {
        File childFile = mock(File.class);
        File childDir = mock(File.class);
        when(file.listFiles(fileFilter)).thenReturn(new File[]{childFile, childDir});
        String childFileName = "childFile";
        when(childFile.getName()).thenReturn(childFileName);
        when(childFile.isFile()).thenReturn(true);
        when(childFile.getParentFile()).thenReturn(file);

        when(childDir.isDirectory()).thenReturn(true);
        File grandChildFile = mock(File.class);
        when(grandChildFile.getParentFile()).thenReturn(childDir);
        String grandChildFileName = "grandChildFile";
        when(grandChildFile.getName()).thenReturn(grandChildFileName);
        when(grandChildFile.isFile()).thenReturn(true);
        when(childDir.listFiles(fileFilter)).thenReturn(new File[]{grandChildFile});

        Iterable<DataPacket> dataPackets = listFileCollector.getDataPackets();
        Iterator<DataPacket> iterator = dataPackets.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(childFileName, iterator.next().getAttributes().get("filename"));
        assertTrue(iterator.hasNext());
        assertEquals(grandChildFileName, iterator.next().getAttributes().get("filename"));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void testModifiedFilter() throws InterruptedException {
        final long currentTimeMillis = System.currentTimeMillis();
        final String validName = "valid";

        when(fileFilter.accept(any(File.class))).thenReturn(true);
        when(file.listFiles(any(FileFilter.class))).thenAnswer(new Answer<File[]>() {
            @Override
            public File[] answer(InvocationOnMock invocationOnMock) throws Throwable {
                File tooOld = mock(File.class);
                when(tooOld.lastModified()).thenReturn(currentTimeMillis - 1);
                when(tooOld.isFile()).thenReturn(true);

                File  valid = mock(File.class);
                when(valid.getName()).thenReturn(validName);
                when(valid.getParentFile()).thenReturn(file);
                when(valid.lastModified()).thenReturn(currentTimeMillis);
                when(valid.isFile()).thenReturn(true);

                File tooNew = mock(File.class);
                when(tooNew.lastModified()).thenReturn(System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1));
                when(tooNew.isFile()).thenReturn(true);
                FileFilter filter = (FileFilter) invocationOnMock.getArguments()[0];
                List<File> result = new ArrayList<File>();
                for (File file : new File[]{tooOld, valid, tooNew}) {
                    if (filter.accept(file)) {
                        result.add(file);
                    }
                }
                return result.toArray(new File[result.size()]);
            }
        });
        listFileCollector = new ListFileCollector(file, fileFilter, currentTimeMillis);
        Thread.sleep(1);

        Iterable<DataPacket> dataPackets = listFileCollector.getDataPackets();
        Iterator<DataPacket> iterator = dataPackets.iterator();
        assertTrue(iterator.hasNext());
        assertEquals(validName, iterator.next().getAttributes().get("filename"));
        assertFalse(iterator.hasNext());
    }
}
