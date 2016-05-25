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

import com.google.common.collect.ImmutableMap;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrQuery.ORDER;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HBaseMapReduceIndexerToolGoLiveTest {

    private static final byte[] TEST_TABLE_NAME = Bytes.toBytes("record");
    private static final byte[] TEST_COLFAM_NAME = Bytes.toBytes("info");
    
    private static final HBaseTestingUtility HBASE_TEST_UTILITY  = HBaseTestingUtilityFactory.createTestUtility();
    private static MRTestUtil MR_TEST_UTIL;
    private static SolrTestingUtility SOLR_TEST_UTILITY;
    
    private static final String RESOURCES_DIR = "target/test-classes";
    private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
    
    private static CloudSolrClient COLLECTION1;
    private static HBaseAdmin HBASE_ADMIN;
    private static HTable RECORD_TABLE;
    private static String SOLR_ZK;
    private static String INDEXER_ZK;
    private static IndexerModelImpl INDEXER_MODEL;
    
    private static final int RECORD_COUNT = 2000;

    
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        MR_TEST_UTIL = new MRTestUtil(HBASE_TEST_UTILITY);
        HBASE_TEST_UTILITY.startMiniCluster();
        MR_TEST_UTIL.startMrCluster();
        
        FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
        int zkClientPort = HBASE_TEST_UTILITY.getZkCluster().getClientPort();
        
        SOLR_TEST_UTILITY = new SolrTestingUtility(zkClientPort, NetUtils.getFreePort(),
                ImmutableMap.of(
                        "solr.hdfs.blockcache.enabled", "false",
                        "solr.directoryFactory", "HdfsDirectoryFactory",
                        "solr.hdfs.home", fs.makeQualified(new Path("/solrdata")).toString()));
        SOLR_TEST_UTILITY.start();
        
        SOLR_TEST_UTILITY.uploadConfig("config1", new File(MINIMR_CONF_DIR, "conf"));
        SOLR_TEST_UTILITY.createCollection("collection1", "config1", 2);
        SOLR_TEST_UTILITY.createCollection("collection2", "config1", 2);

        COLLECTION1 = new CloudSolrClient(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION1.setDefaultCollection("collection1");

        SOLR_ZK = "127.0.0.1:" + zkClientPort + "/solr";
        INDEXER_ZK = "localhost:" + zkClientPort;
        ZooKeeperItf zkItf = ZkUtil.connect(INDEXER_ZK, 15000);
        INDEXER_MODEL = new IndexerModelImpl(zkItf, "/ngdata/hbaseindexer");
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                                                .name("zkindexerdef")
                                                .indexerComponentFactory(DefaultIndexerComponentFactory.class.getName())
                                                .configuration(Resources.toByteArray(Resources.getResource(
                                                        HBaseMapReduceIndexerToolGoLiveTest.class, "user_indexer.xml")))
                                                .connectionParams(ImmutableMap.of(
                                                        "solr.zk", SOLR_ZK,
                                                        "solr.collection", "collection1"))
                                                .build();

        addAndWaitForIndexer(indexerDef);
        
        Closer.close(zkItf);
        
        HTableDescriptor tableDescriptor = new HTableDescriptor(TEST_TABLE_NAME);
        tableDescriptor.addFamily(new HColumnDescriptor(TEST_COLFAM_NAME));
        HBASE_ADMIN = new HBaseAdmin(HBASE_TEST_UTILITY.getConfiguration());
        HBASE_ADMIN.createTable(tableDescriptor, new byte[][]{Bytes.toBytes("row0800"), Bytes.toBytes("row1600")});
        
        RECORD_TABLE = new HTable(HBASE_TEST_UTILITY.getConfiguration(), TEST_TABLE_NAME);
        
        for (int i = 0; i < RECORD_COUNT; i++) {
            writeHBaseRecord(String.format("row%04d", i), ImmutableMap.of(
                    "firstname", String.format("John%04d", i),
                    "lastname", String.format("Doe%04d", i)));
        }
        
        
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        SOLR_TEST_UTILITY.stop();
        HBASE_ADMIN.close();
        HBASE_TEST_UTILITY.shutdownMiniMapReduceCluster();
        HBASE_TEST_UTILITY.shutdownMiniCluster();
    }
    
    @After
    public void tearDown() throws IOException, SolrServerException {
        
        COLLECTION1.deleteByQuery("*:*");
        COLLECTION1.commit();
        
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
    private static void writeHBaseRecord(String row, Map<String,String> qualifiersAndValues) throws IOException {
        Put put = new Put(Bytes.toBytes(row));
        for (Entry<String, String> entry : qualifiersAndValues.entrySet()) {
            put.add(TEST_COLFAM_NAME, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue()));
        }
        RECORD_TABLE.put(put);
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
        SolrQuery query = new SolrQuery(queryString).setRows(RECORD_COUNT * 2).addSort("id", ORDER.asc);
        QueryResponse response = collection.query(query);
        return response.getResults();
    }
    
    private void verifySolrContents() throws Exception {

        // verify query
        assertEquals(RECORD_COUNT, executeSolrQuery("*:*").getNumFound());
        assertEquals(1, executeSolrQuery("firstname_s:John0001").getNumFound());
        int i = 0;
        for (SolrDocument doc : executeSolrQuery("*:*")) {
            assertEquals(String.format("row%04d", i), doc.getFirstValue("id"));
            assertEquals(String.format("John%04d", i), doc.getFirstValue("firstname_s"));
            assertEquals(String.format("Doe%04d", i), doc.getFirstValue("lastname_s"));
            
            // perform update
            doc.removeFields("_version_");
            SolrInputDocument update = new SolrInputDocument();
            for (Map.Entry<String, Object> entry : doc.entrySet()) {
                update.setField(entry.getKey(), entry.getValue());
            }
            update.setField("firstname_s", String.format("Nadja%04d", i));
            COLLECTION1.add(update);
            i++;
        }
        assertEquals(RECORD_COUNT, i);
        COLLECTION1.commit();
        
        // verify updates
        assertEquals(RECORD_COUNT, executeSolrQuery("*:*").getNumFound());
        i = 0;
        for (SolrDocument doc : executeSolrQuery("*:*")) {
            assertEquals(String.format("row%04d", i), doc.getFirstValue("id"));
            assertEquals(String.format("Nadja%04d", i), doc.getFirstValue("firstname_s"));
            assertEquals(String.format("Doe%04d", i), doc.getFirstValue("lastname_s"));
            
            // perform delete
            COLLECTION1.deleteById((String)doc.getFirstValue("id"));
            i++;
        }
        assertEquals(RECORD_COUNT, i);
        COLLECTION1.commit();
        
        // verify deletes
        assertEquals(0, executeSolrQuery("*:*").size());
    }
  
    @Test
    public void testIndexer_GoLive() throws Exception {

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--go-live",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK);

        verifySolrContents();
    }
    
    @Test
    public void testIndexer_WithMerge() throws Exception {

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--go-live",
                "--reducers", "3",
                "--collection", "collection1",
                "--zk-host", SOLR_ZK);
        
        verifySolrContents();
    }
    
    @Test
    public void testIndexer_Morphlines() throws Exception {
        
        File indexerConfigFile = MRTestUtil.substituteZkHost(
            new File(Resources.getResource("morphline_indexer.xml").toURI()), SOLR_TEST_UTILITY.getZkConnectString());

        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", indexerConfigFile.toString(),
            "--reducers", "4",
            "--fanout", "2",
            "--zk-host", SOLR_ZK,
            "--collection", "collection1",
                "--log4j", new File(Resources.getResource("log4j.properties").toURI()).toString(),
            "--go-live-threads", "999",
            "--go-live");
        
        verifySolrContents();
    }
    
    @Test
    public void testIndexer_MorphlinesWithOneReducerPerShard() throws Exception {
        
        File indexerConfigFile = MRTestUtil.substituteZkHost(
            new File(Resources.getResource("morphline_indexer.xml").toURI()), SOLR_TEST_UTILITY.getZkConnectString());

        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", indexerConfigFile.toString(),
            "--reducers", "-2",
            "--fanout", "2",
            "--zk-host", SOLR_ZK,
            "--collection", "collection1",
                "--log4j", new File(Resources.getResource("log4j.properties").toURI()).toString(),
            "--go-live-threads", "999",
            "--go-live");
        
        verifySolrContents();
    }
    
    @Test
    public void testIndexer_WithUserSuppliedSolrDir() throws Exception {

        MR_TEST_UTIL.runTool(
                "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
                "--go-live",
                "--collection", "collection1",
                "--solr-home-dir", MINIMR_CONF_DIR.toString(),
                "--zk-host", SOLR_ZK);
        
        verifySolrContents();
    }
    
    @Test
    public void testIndexer_IndexerFromZooKeeper() throws Exception {
        MR_TEST_UTIL.runTool(
                "--hbase-indexer-name", "zkindexerdef",
                "--hbase-indexer-zk", INDEXER_ZK,
                "--go-live");
        
        verifySolrContents();
    }
}
