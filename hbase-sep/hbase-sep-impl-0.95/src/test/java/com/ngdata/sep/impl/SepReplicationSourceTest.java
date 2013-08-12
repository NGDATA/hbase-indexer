/*
 * Copyright 2012 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.sep.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.NavigableMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.sep.WALEditFilter;
import com.ngdata.sep.WALEditFilterProvider;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class SepReplicationSourceTest {

    private SepReplicationSource sepReplicationSource;

    @Before
    public void setUp() {
        sepReplicationSource = new SepReplicationSource();
    }

    @Test
    public void testLoadEditFilter_CustomFilterAvailable() {
        final String subscriptionId = "_subscription_id_";

        WALEditFilter editFilter = mock(WALEditFilter.class);
        WALEditFilterProvider filterProvider = mock(WALEditFilterProvider.class);
        when(filterProvider.getWALEditFilter(subscriptionId)).thenReturn(editFilter);

        WALEditFilter loadedFilter = sepReplicationSource.loadEditFilter(subscriptionId,
                Lists.newArrayList(filterProvider));

        assertEquals(editFilter, loadedFilter);
    }

    @Test
    public void testLoadEditFilter_NoApplicableFilterAvailable() {
        final String subscriptionId = "_subscription_id_";

        WALEditFilterProvider editFilterProvider = mock(WALEditFilterProvider.class);
        when(editFilterProvider.getWALEditFilter(subscriptionId)).thenReturn(null);

        WALEditFilter loadedFilter = sepReplicationSource.loadEditFilter(subscriptionId,
                Lists.newArrayList(editFilterProvider));

        assertNull(loadedFilter);
    }

    @Test
    public void testRemoveNonReplicableEdits_CustomFilter() {
        WALEditFilter editFilter = mock(WALEditFilter.class);
        sepReplicationSource.setWALEditFilter(editFilter);

        WALEdit walEdit = new WALEdit();
        KeyValue keyValue = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qual"),
                Bytes.toBytes("value"));
        NavigableMap<byte[], Integer> scopes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        scopes.put(Bytes.toBytes("cf"), 1);
        walEdit.setScopes(scopes);
        walEdit.add(keyValue);

        sepReplicationSource.removeNonReplicableEdits(walEdit);

        verify(editFilter).apply(walEdit);
    }

    @Test
    public void testRemoveNonReplicableEdits_NoCustomFilter() {
        sepReplicationSource.setWALEditFilter(null);

        WALEdit walEdit = new WALEdit();
        KeyValue keyValue = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qual"),
                Bytes.toBytes("value"));
        NavigableMap<byte[], Integer> scopes = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);
        scopes.put(Bytes.toBytes("cf"), 1);
        walEdit.setScopes(scopes);
        walEdit.add(keyValue);

        sepReplicationSource.removeNonReplicableEdits(walEdit);

        assertEquals(1, walEdit.size());
    }

}
