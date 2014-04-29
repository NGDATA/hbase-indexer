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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.zookeeper.ZkConnectException;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class SepConsumerIT {

    private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
    private static final byte[] DATA_COL_FAMILY = Bytes.toBytes("datacf");
    private static final byte[] PAYLOAD_COL_FAMILY = Bytes.toBytes("payloadcf");
    private static final byte[] PAYLOAD_COL_QUALIFIER = Bytes.toBytes("payload_qualifier");
    private static final String SUBSCRIPTION_NAME = "test_subscription";
    private static final String SUBSCRIPTION_WITH_PAYLOADS_NAME = "test_subscription_with_payloads";
    private static final int WAIT_TIMEOUT = 10000;

    private static Configuration clusterConf;
    private static HBaseTestingUtility hbaseTestUtil;
    private static HBaseAdmin hbaseAdmin;
    private static HTable htable;

    private ZooKeeperItf zkItf;
    private SepModel sepModel;
    private SepConsumer sepConsumer;
    private SepConsumer sepConsumerWithPayloads;
    private TestEventListener eventListener;
    private TestEventListener eventListenerWithPayloads;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        clusterConf = HBaseConfiguration.create();
        clusterConf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
        clusterConf.setLong("replication.source.sleepforretries", 50);
        clusterConf.set("replication.replicationsource.implementation", SepReplicationSource.class.getName());
        clusterConf.setInt("hbase.master.info.port", -1);
        clusterConf.setInt("hbase.regionserver.info.port", -1);

        hbaseTestUtil = new HBaseTestingUtility(clusterConf);
        
        hbaseTestUtil.startMiniZKCluster(1);
        hbaseTestUtil.startMiniCluster(1);

        HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
        HColumnDescriptor dataColfamDescriptor = new HColumnDescriptor(DATA_COL_FAMILY);
        dataColfamDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
        HColumnDescriptor payloadColfamDescriptor = new HColumnDescriptor(PAYLOAD_COL_FAMILY);
        payloadColfamDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
        tableDescriptor.addFamily(dataColfamDescriptor);
        tableDescriptor.addFamily(payloadColfamDescriptor);

        hbaseAdmin = new HBaseAdmin(clusterConf);
        hbaseAdmin.createTable(tableDescriptor);
        htable = new HTable(clusterConf, TABLE_NAME);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        hbaseAdmin.close();
        hbaseTestUtil.shutdownMiniCluster();
    }

    @Before
    public void setUp() throws ReplicationException, ZkConnectException, InterruptedException, KeeperException, IOException {
        zkItf = ZkUtil.connect("localhost:" + hbaseTestUtil.getZkCluster().getClientPort(), 30000);
        sepModel = new SepModelImpl(zkItf, clusterConf);
        sepModel.addSubscription(SUBSCRIPTION_NAME);
        sepModel.addSubscription(SUBSCRIPTION_WITH_PAYLOADS_NAME);
        eventListener = new TestEventListener();
        eventListenerWithPayloads = new TestEventListener();
        sepConsumer = new SepConsumer(SUBSCRIPTION_NAME, System.currentTimeMillis(), eventListener, 3, "localhost",
                zkItf, clusterConf);
        PayloadExtractor payloadExtractor = new BasePayloadExtractor(TABLE_NAME, PAYLOAD_COL_FAMILY, PAYLOAD_COL_QUALIFIER);
        sepConsumerWithPayloads = new SepConsumer(SUBSCRIPTION_WITH_PAYLOADS_NAME, System.currentTimeMillis(), eventListenerWithPayloads, 3, "localhost", zkItf, clusterConf, payloadExtractor);
        sepConsumer.start();
        sepConsumerWithPayloads.start();
    }

    @After
    public void tearDown() throws ReplicationException, IOException {
        sepConsumer.stop();
        sepConsumerWithPayloads.stop();
        sepModel.removeSubscription(SUBSCRIPTION_NAME);
        sepModel.removeSubscription(SUBSCRIPTION_WITH_PAYLOADS_NAME);
        zkItf.close();
    }

    @Test
    public void testEvents_SimpleSetOfPuts() throws IOException, InterruptedException {
        for (int i = 0; i < 3; i++) {
            Put put = new Put(Bytes.toBytes("row " + i));
            put.add(DATA_COL_FAMILY, Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
            htable.put(put);
        }
        waitForEvents(eventListener, 3);
        waitForEvents(eventListenerWithPayloads, 3);

        assertEquals(3, eventListener.getEvents().size());
        assertEquals(3, eventListenerWithPayloads.getEvents().size());

        Set<String> rowKeys = Sets.newHashSet();
        for (SepEvent sepEvent : eventListener.getEvents()) {
            rowKeys.add(Bytes.toString(sepEvent.getRow()));
        }
        assertEquals(Sets.newHashSet("row 0", "row 1", "row 2"), rowKeys);
    }

    @Test
    public void testEvents_MultipleKeyValuesInSinglePut() throws IOException {
        Put putA = new Put(Bytes.toBytes("rowA"));
        Put putB = new Put(Bytes.toBytes("rowB"));

        putA.add(DATA_COL_FAMILY, Bytes.toBytes("a1"), Bytes.toBytes("valuea1"));
        putA.add(DATA_COL_FAMILY, Bytes.toBytes("a2"), Bytes.toBytes("valuea2"));

        putB.add(DATA_COL_FAMILY, Bytes.toBytes("b1"), Bytes.toBytes("valueb1"));
        putB.add(DATA_COL_FAMILY, Bytes.toBytes("b2"), Bytes.toBytes("valueb2"));
        putB.add(DATA_COL_FAMILY, Bytes.toBytes("b3"), Bytes.toBytes("valueb3"));

        htable.put(putA);
        htable.put(putB);

        waitForEvents(eventListener, 2);
        waitForEvents(eventListenerWithPayloads, 2);
        
        assertEquals(2, eventListenerWithPayloads.getEvents().size());
        
        List<SepEvent> events = eventListener.getEvents();

        assertEquals(2, events.size());

        SepEvent eventA, eventB;
        if ("rowA".equals(Bytes.toString(events.get(0).getRow()))) {
            eventA = events.get(0);
            eventB = events.get(1);
        } else {
            eventA = events.get(1);
            eventB = events.get(0);
        }
        
        assertEquals("rowA", Bytes.toString(eventA.getRow()));
        assertEquals("rowB", Bytes.toString(eventB.getRow()));
    }
    
    @Test
    public void testEvents_WithPayload() throws IOException {
        Put put = new Put(Bytes.toBytes("rowkey"));
        put.add(DATA_COL_FAMILY, Bytes.toBytes("data"), Bytes.toBytes("value"));
        put.add(PAYLOAD_COL_FAMILY, PAYLOAD_COL_QUALIFIER, Bytes.toBytes("payload"));
        htable.put(put);
        
        waitForEvents(eventListener, 1);
        waitForEvents(eventListenerWithPayloads, 1);
        
        SepEvent eventWithoutPayload = eventListener.getEvents().get(0);
        SepEvent eventWithPayload = eventListenerWithPayloads.getEvents().get(0);
        
        assertEquals("rowkey", Bytes.toString(eventWithoutPayload.getRow()));
        assertEquals("rowkey", Bytes.toString(eventWithPayload.getRow()));
        
        assertEquals(2, eventWithoutPayload.getKeyValues().size());
        assertEquals(2, eventWithPayload.getKeyValues().size());
        
        assertNull(eventWithoutPayload.getPayload());
        assertEquals("payload", Bytes.toString(eventWithPayload.getPayload()));
    }
    
    static String getDefaultUmask() throws IOException {
        // Hack to get around the test DFS cluster only wanting to start up if the
        // umask is set to the expected value (i.e. 022)
        File tmpDir = Files.createTempDir();
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        FileStatus stat = local.getFileStatus(new Path(tmpDir.getAbsolutePath()));
        FsPermission permission = stat.getPermission();
        String permString = Integer.toString(permission.toShort(), 8);
        tmpDir.delete();
        return permString;
    }

    private void waitForEvents(TestEventListener listener, int expectedNumEvents) throws IOException {
        long start = System.currentTimeMillis();
        while (listener.getEvents().size() < expectedNumEvents) {
            if (System.currentTimeMillis() - start > WAIT_TIMEOUT) {
                throw new RuntimeException("Waited too long on " + expectedNumEvents + ", only have "
                        + listener.getEvents().size() + " after " + WAIT_TIMEOUT + " milliseconds");
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }

    static class TestEventListener implements EventListener {

        private List<SepEvent> sepEvents = Collections.synchronizedList(Lists.<SepEvent> newArrayList());

        @Override
        public synchronized void processEvents(List<SepEvent> events) {
            sepEvents.addAll(events);
        }

        public List<SepEvent> getEvents() {
            return Collections.unmodifiableList(sepEvents);
        }

        public void reset() {
            sepEvents.clear();
        }

    }

}
