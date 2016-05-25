/*
 * Copyright 2013 NGDATA nv
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
package com.ngdata.hbaseindexer.mr;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.util.net.NetUtils;
import com.ngdata.hbaseindexer.util.solr.SolrTestingUtility;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseMapReduceIndexerToolDirectWriteTest {

    private static final byte[] TEST_TABLE_NAME = Bytes.toBytes("record");
    private static final byte[] TEST_COLFAM_NAME = Bytes.toBytes("info");
    
    private static final HBaseTestingUtility HBASE_TEST_UTILITY = HBaseTestingUtilityFactory.createTestUtility();
    private static MRTestUtil MR_TEST_UTIL;
    private static SolrTestingUtility SOLR_TEST_UTILITY;
    
    
    private static CloudSolrClient COLLECTION1;
    private static CloudSolrClient COLLECTION2;
    private static HBaseAdmin HBASE_ADMIN;
    private static String SOLR_ZK;
    private static String INDEXER_ZK;
    private static IndexerModelImpl INDEXER_MODEL;

    private HTable recordTable;
    
    private Configuration indexerToolConf;
    
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        MR_TEST_UTIL = new MRTestUtil(HBASE_TEST_UTILITY);
        HBASE_TEST_UTILITY.startMiniCluster();
        MR_TEST_UTIL.startMrCluster();
        
        int zkClientPort = HBASE_TEST_UTILITY.getZkCluster().getClientPort();
        
        SOLR_TEST_UTILITY = new SolrTestingUtility(zkClientPort, NetUtils.getFreePort());
        SOLR_TEST_UTILITY.start();
        SOLR_TEST_UTILITY.uploadConfig("config1",
                Resources.toByteArray(Resources.getResource(HBaseMapReduceIndexerToolDirectWriteTest.class, "schema.xml")),
                Resources.toByteArray(Resources.getResource(HBaseMapReduceIndexerToolDirectWriteTest.class, "solrconfig.xml")));
        SOLR_TEST_UTILITY.createCore("collection1_core1", "collection1", "config1", 1);
        SOLR_TEST_UTILITY.createCore("collection2_core1", "collection2", "config1", 1);

        COLLECTION1 = new CloudSolrClient(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION1.setDefaultCollection("collection1");

        COLLECTION2 = new CloudSolrClient(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION2.setDefaultCollection("collection2");
        
        SOLR_ZK = "127.0.0.1:" + zkClientPort + "/solr";
        INDEXER_ZK = "localhost:" + zkClientPort;
        ZooKeeperItf zkItf = ZkUtil.connect(INDEXER_ZK, 15000);
        INDEXER_MODEL = new IndexerModelImpl(zkItf, "/ngdata/hbaseindexer");
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
        .name("zkindexerdef")
        .indexerComponentFactory(DefaultIndexerComponentFactory.class.getName())
        .configuration(Resources.toByteArray(Resources.getResource(
                HBaseMapReduceIndexerToolDirectWriteTest.class, "user_indexer.xml")))
        .connectionParams(ImmutableMap.of(
                "solr.zk", SOLR_ZK,
                "solr.collection", "collection1"))
        .build();

        addAndWaitForIndexer(indexerDef);
        
        Closer.close(zkItf);
        
        HBASE_ADMIN = new HBaseAdmin(HBASE_TEST_UTILITY.getConfiguration());
        
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        SOLR_TEST_UTILITY.stop();
        HBASE_ADMIN.close();
        HBASE_TEST_UTILITY.shutdownMiniMapReduceCluster();
        HBASE_TEST_UTILITY.shutdownMiniCluster();
    }
    
    @Before
    public void setUp() throws Exception {
        createHTable(TEST_TABLE_NAME);
        recordTable = new HTable(HBASE_TEST_UTILITY.getConfiguration(), TEST_TABLE_NAME);
        indexerToolConf = HBASE_TEST_UTILITY.getConfiguration();
    }
    
    @After
    public void tearDown() throws IOException, SolrServerException {
        HBASE_ADMIN.disableTable(TEST_TABLE_NAME);
        HBASE_ADMIN.deleteTable(TEST_TABLE_NAME);
        
        recordTable.close();
        
        COLLECTION1.deleteByQuery("*:*");
        COLLECTION1.commit();
        
        COLLECTION2.deleteByQuery("*:*");
        COLLECTION2.commit();
        
        // Be extra sure Solr is empty now
        QueryResponse response = COLLECTION1.query(new SolrQuery("*:*"));
        assertTrue(response.getResults().isEmpty());
    }
    
    private static void addAndWaitForIndexer(IndexerDefinition indexerDef) throws Exception {
        long startTime = System.currentTimeMillis();
        INDEXER_MODEL.addIndexer(indexerDef);
        
        // Wait max 5 seconds
        while (System.currentTimeMillis() - startTime < 15000) {
            if (INDEXER_MODEL.hasIndexer(indexerDef.getName())) {
                return;
            }
            Thread.sleep(200);
        }
        throw new RuntimeException("Failed to add indexer: " + indexerDef);
    }

    /**
     * Write String values to HBase. Direct string-to-bytes encoding is used for
     * writing all values to HBase. All values are stored in the TEST_COLFAM_NAME
     * column family.
     *
     *
     * @param row row key under which are to be stored
     * @param qualifiersAndValues map of column qualifiers to cell values
     */
    private void writeHBaseRecord(String row, Map<String,String> qualifiersAndValues) throws IOException {
        writeHBaseRecord(row, qualifiersAndValues, recordTable);
    }

    /**
     * Write String values to HBase. Direct string-to-bytes encoding is used for
     * writing all values to HBase. All values are stored in the TEST_COLFAM_NAME
     * column family.
     *
     *
     * @param row row key under which are to be stored
     * @param qualifiersAndValues map of column qualifiers to cell values
     * @param table htable to write to
     */
     private static void writeHBaseRecord(String row, Map<String,String> qualifiersAndValues, HTable table) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        for (Entry<String, String> entry : qualifiersAndValues.entrySet()) {
            put.add(TEST_COLFAM_NAME, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        table.put(put);
     }
    
    /**
     * Execute a Solr query on COLLECTION1.
     * 
     * @param queryString Solr query string
     * @return list of results from Solr
     */
    private SolrDocumentList executeSolrQuery(String queryString) throws SolrServerException, IOException {
        return executeSolrQuery(COLLECTION1, queryString);
    }
    
    /**
     * Execute a Solr query on a specific collection.
     */
    private SolrDocumentList executeSolrQuery(CloudSolrClient collection, String queryString) throws SolrServerException, IOException {
        QueryResponse response = collection.query(new SolrQuery(queryString));
        return response.getResults();
    }
    
    @Test
    public void testIndexer_DirectWrite() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK);
        
        assertEquals(1, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_ZkBasedIndexerDefinition() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-name", "zkindexerdef",
                "--hbase-indexer-zk", INDEXER_ZK,
                "--reducers", "0");
        
        assertEquals(1, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_Morphline() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        indexerToolConf.set("morphlineField.forcedMoo", "forcedBaz");
        indexerToolConf.set("morphlineVariable.myFoo", "myBar");
        File indexerConfigFile = MRTestUtil.substituteZkHost(
            new File("target/test-classes/morphline_indexer.xml"), SOLR_TEST_UTILITY.getZkConnectString());
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", indexerConfigFile.toString(),
                "--morphline-file", new File("src/test/resources/extractHBaseCell.conf").toString(),
                "--morphline-id", "morphline1",
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK);
               
        assertEquals(1, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_Morphline_With_DryRun() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        indexerToolConf.set("morphlineField.forcedMoo", "forcedBaz");
        indexerToolConf.set("morphlineVariable.myFoo", "myBar");
        File indexerConfigFile = MRTestUtil.substituteZkHost(
            new File("target/test-classes/morphline_indexer.xml"), SOLR_TEST_UTILITY.getZkConnectString());
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", indexerConfigFile.toString(),
                "--morphline-file", new File("src/test/resources/extractHBaseCell.conf").toString(),
                "--morphline-id", "morphline1",
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--dry-run");
        
        assertEquals(0, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
    }
    
    @Test
    public void testIndexer_AlternateCollection() throws Exception {
        writeHBaseRecord("row1", ImmutableMap.of(
                                "firstname", "John",
                                "lastname", "Doe"));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection2",
                "--zk-host", SOLR_ZK);
        
        String solrQuery = "firstname_s:John lastname_s:Doe";
        
        assertTrue(executeSolrQuery(COLLECTION1, solrQuery).isEmpty());
        assertEquals(1, executeSolrQuery(COLLECTION2, solrQuery).size());
    }

    @Test
    public void testIndexer_ClearIndex() throws Exception {
        indexClearTester(true);
    }

    @Test
    public void testIndexer_NoClearIndex() throws Exception {
        indexClearTester(false);
    }

    private void indexClearTester(boolean clear) throws Exception{
        SolrInputDocument solrDoc = new SolrInputDocument();
        solrDoc.addField("id", "nomatter");
        solrDoc.addField("firstname_s", "John");
        solrDoc.addField("lastname_s", "Doe");

        COLLECTION1.add(solrDoc);
        COLLECTION1.commit();

        String solrQuery = "firstname_s:John lastname_s:Doe";

        assertEquals(1, executeSolrQuery(COLLECTION1, solrQuery).size());

        writeHBaseRecord("row1", ImmutableMap.of(
                "firstname", "John",
                "lastname", "Doe"));

        List<String> args = Lists.newArrayList(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK);

        if (clear) {
            args.add("--clear-index");
        }

        MR_TEST_UTIL.runTool(args.toArray(new String[args.size()]));

        if (clear) {
            assertEquals(1, executeSolrQuery(COLLECTION1, solrQuery).size());
        } else {
            assertEquals(2, executeSolrQuery(COLLECTION1, solrQuery).size());
        }

    }
    
    @Test
    public void testIndexer_StartRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-start-row", "b");
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Aaron").isEmpty());
        
    }
    
    @Test
    public void testIndexer_EndRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-end-row", "c");
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Carl").isEmpty());
    }
    
    @Test
    public void testIndexer_StartAndEndRowDefined() throws Exception {
        writeHBaseRecord("a", ImmutableMap.of("firstname", "Aaron"));
        writeHBaseRecord("b", ImmutableMap.of("firstname", "Brian"));
        writeHBaseRecord("c", ImmutableMap.of("firstname", "Carl"));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-start-row", "b",
                "--hbase-end-row", "c");
        
        assertEquals(1, executeSolrQuery("*:*").size());
        assertEquals(1, executeSolrQuery("firstname_s:Brian").size());
    }
    
    @Test
    public void testIndexer_StartTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-start-time", "2");
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Early").isEmpty());
    }
    
    @Test
    public void testIndexer_EndTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));
        
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-end-time", "3");
        
        assertEquals(2, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Late").isEmpty());
    }
    
    @Test
    public void testIndexer_StartAndEndTimeDefined() throws Exception {
        Put putEarly = new Put(Bytes.toBytes("early"));
        putEarly.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 1L, Bytes.toBytes("Early"));
        
        Put putOntime = new Put(Bytes.toBytes("ontime"));
        putOntime.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 2L, Bytes.toBytes("Ontime"));

        Put putLate = new Put(Bytes.toBytes("late"));
        putLate.add(TEST_COLFAM_NAME, Bytes.toBytes("firstname"), 3L, Bytes.toBytes("Late"));
        
        recordTable.put(ImmutableList.of(putEarly, putOntime, putLate));

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--reducers", "0",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK,
                "--hbase-start-time", "2",
                "--hbase-end-time", "3");
        
        assertEquals(1, executeSolrQuery("*:*").size());
        assertTrue(executeSolrQuery("firstname_s:Early").isEmpty());
        assertEquals(1, executeSolrQuery("firstname_s:Ontime").size());
        assertTrue(executeSolrQuery("firstname_s:Late").isEmpty());
    }

    @Test
    public void testIndexer_Multitable() throws Exception {
        String tablePrefix = "_multitable_";
        HTableDescriptor descriptorA = createHTable((tablePrefix + "a_").getBytes(Charsets.UTF_8));
        HTableDescriptor descriptorB = createHTable((tablePrefix + "b_").getBytes(Charsets.UTF_8));
        HTable recordTable2 = new HTable(HBASE_TEST_UTILITY.getConfiguration(), tablePrefix + "a_");
        HTable recordTable3 =  new HTable(HBASE_TEST_UTILITY.getConfiguration(), tablePrefix + "b_");

        String hbaseTableName = tablePrefix + ".*";
        try {
            writeHBaseRecord("row1", ImmutableMap.of(
                    "firstname", "John",
                    "lastname", "Doe"), recordTable2);
            writeHBaseRecord("row2", ImmutableMap.of(
                    "firstname", "John",
                    "lastname", "Doe"), recordTable3);

            MR_TEST_UTIL.runTool(
                    "--hbase-indexer-file", new File(Resources.getResource(getClass(), "multitable_indexer.xml").toURI()).toString(),
                    "--reducers", "0",
                    "--collection", "collection1",
                    "--zk-host", SOLR_ZK);

            assertEquals(2, executeSolrQuery("firstname_s:John lastname_s:Doe").size());
        } finally {
            HBASE_ADMIN.disableTables(hbaseTableName);
            HBASE_ADMIN.deleteTables(hbaseTableName);

            recordTable2.close();
            recordTable3.close();
        }
    }

    private static HTableDescriptor createHTable (byte[] tableName) throws Exception{
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor(TEST_COLFAM_NAME));
        HBASE_ADMIN.createTable(tableDescriptor);

        return tableDescriptor;
    }
    
}
