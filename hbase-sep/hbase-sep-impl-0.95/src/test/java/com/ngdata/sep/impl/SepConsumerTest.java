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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.Lists;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class SepConsumerTest {

    private static final long SUBSCRIPTION_TIMESTAMP = 100000;
    
    private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
    private static final byte[] DATA_COLFAM = Bytes.toBytes("data");
    private static final byte[] PAYLOAD_QUALIFIER = Bytes.toBytes("pl");
    private static final byte[] encodedRegionName = Bytes.toBytes("1028785192");
    private static final UUID clusterUUID = UUID.randomUUID();
    private static final List<UUID> clusterUUIDs = new ArrayList<UUID>();

    private EventListener eventListener;
    private ZooKeeperItf zkItf;
    private SepConsumer sepConsumer;

    @Before
    public void setUp() throws IOException, InterruptedException, KeeperException {
        eventListener = mock(EventListener.class);
        zkItf = mock(ZooKeeperItf.class);
        PayloadExtractor payloadExtractor = new BasePayloadExtractor(TABLE_NAME, DATA_COLFAM, PAYLOAD_QUALIFIER);
        sepConsumer = new SepConsumer("subscriptionId", SUBSCRIPTION_TIMESTAMP, eventListener, 1, "localhost", zkItf,
                HBaseConfiguration.create(), payloadExtractor);
    }

    @After
    public void tearDown() {
        sepConsumer.stop();
    }

    private HLog.Entry createHlogEntry(byte[] tableName, KeyValue... keyValues) {
        return createHlogEntry(tableName, SUBSCRIPTION_TIMESTAMP + 1, keyValues);
    }

    private HLog.Entry createHlogEntry(byte[] tableName, long writeTime, KeyValue... keyValues) {
        HLog.Entry entry = mock(HLog.Entry.class, Mockito.RETURNS_DEEP_STUBS);
        when(entry.getEdit().getKeyValues()).thenReturn(Lists.newArrayList(keyValues));
        when(entry.getKey().getTablename()).thenReturn(TableName.valueOf(tableName));
        when(entry.getKey().getWriteTime()).thenReturn(writeTime);
        when(entry.getKey().getEncodedRegionName()).thenReturn(encodedRegionName);
        when(entry.getKey().getClusterIds()).thenReturn(clusterUUIDs);
        return entry;
    }

    @Test
    public void testReplicateLogEntries() throws IOException {

        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadData = Bytes.toBytes("payload");

        HLog.Entry hlogEntry = createHlogEntry(TABLE_NAME, new KeyValue(rowKey, DATA_COLFAM,
                PAYLOAD_QUALIFIER, payloadData));

        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{hlogEntry});

        SepEvent expectedSepEvent = new SepEvent(TABLE_NAME, rowKey,
                hlogEntry.getEdit().getKeyValues(), payloadData);

        verify(eventListener).processEvents(Lists.newArrayList(expectedSepEvent));
    }

    @Test
    public void testReplicateLogEntries_EntryTimestampBeforeSubscriptionTimestamp() throws IOException {
        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadDataBeforeTimestamp = Bytes.toBytes("payloadBeforeTimestamp");
        byte[] payloadDataOnTimestamp = Bytes.toBytes("payloadOnTimestamp");
        byte[] payloadDataAfterTimestamp = Bytes.toBytes("payloadAfterTimestamp");

        HLog.Entry hlogEntryBeforeTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP - 1,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataBeforeTimestamp));
        HLog.Entry hlogEntryOnTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataOnTimestamp));
        HLog.Entry hlogEntryAfterTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP + 1,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataAfterTimestamp));

        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{hlogEntryBeforeTimestamp});
        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{hlogEntryOnTimestamp});
        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{hlogEntryAfterTimestamp});

        SepEvent expectedEventOnTimestamp = new SepEvent(TABLE_NAME, rowKey,
                hlogEntryOnTimestamp.getEdit().getKeyValues(), payloadDataOnTimestamp);
        SepEvent expectedEventAfterTimestamp = new SepEvent(TABLE_NAME, rowKey,
                hlogEntryAfterTimestamp.getEdit().getKeyValues(), payloadDataAfterTimestamp);

        // Event should be published for data on or after the subscription timestamp, but not before
        verify(eventListener, times(1)).processEvents(Lists.newArrayList(expectedEventOnTimestamp));
        verify(eventListener, times(1)).processEvents(Lists.newArrayList(expectedEventAfterTimestamp));
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testReplicateLogEntries_MultipleKeyValuesForSingleRow() throws Exception {
        byte[] rowKey = Bytes.toBytes("rowKey");

        KeyValue kvA = new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, Bytes.toBytes("A"));
        KeyValue kvB = new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, Bytes.toBytes("B"));

        HLog.Entry entry = createHlogEntry(TABLE_NAME, kvA, kvB);

        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{entry});

        // We should get the first payload in our event (and the second one will be ignored, although the KeyValue will
        // be present in the event
        SepEvent expectedEvent = new SepEvent(TABLE_NAME, rowKey, Lists.newArrayList(kvA, kvB),
                Bytes.toBytes("A"));

        verify(eventListener).processEvents(Lists.newArrayList(expectedEvent));
    }

    // A multi-put could result in updates to multiple rows being included in a single WALEdit.
    // In this case, the KeyValues for separate rows should be separated and result in separate events.
    @Test
    public void testReplicateLogEntries_SingleWALEditForMultipleRows() throws IOException {

        byte[] rowKeyA = Bytes.toBytes("A");
        byte[] rowKeyB = Bytes.toBytes("B");
        byte[] data = Bytes.toBytes("data");

        KeyValue kvA = new KeyValue(rowKeyA, DATA_COLFAM, PAYLOAD_QUALIFIER, data);
        KeyValue kvB = new KeyValue(rowKeyB, DATA_COLFAM, PAYLOAD_QUALIFIER, data);

        HLog.Entry entry = createHlogEntry(TABLE_NAME, kvA, kvB);

        ReplicationProtbufUtil.replicateWALEntry(sepConsumer, new HLog.Entry[]{entry});

        SepEvent expectedEventA = new SepEvent(TABLE_NAME, rowKeyA, Lists.newArrayList(kvA),
                Bytes.toBytes("data"));
        SepEvent expectedEventB = new SepEvent(TABLE_NAME, rowKeyB, Lists.newArrayList(kvB),
                Bytes.toBytes("data"));

        verify(eventListener).processEvents(Lists.newArrayList(expectedEventA, expectedEventB));
    }
}
