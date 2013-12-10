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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.format.DateTimeFormat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class HBaseIndexingOptionsTest {

    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;
    private static ZooKeeperItf ZK;
    private static IndexerModelImpl INDEXER_MODEL;


    private Configuration conf;
    private HBaseIndexingOptions opts;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "hbaseindexer.zktest");
        ZK_CLIENT_PORT = getFreePort();

        ZK_CLUSTER = new MiniZooKeeperCluster();
        ZK_CLUSTER.setDefaultClientPort(ZK_CLIENT_PORT);
        ZK_CLUSTER.startup(ZK_DIR);
        
        ZK = ZkUtil.connect("localhost:" + ZK_CLIENT_PORT, 15000);
        INDEXER_MODEL = new IndexerModelImpl(ZK, "/ngdata/hbaseindexer");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        
        INDEXER_MODEL.stop();
        Closer.close(ZK);
        
        if (ZK_CLUSTER != null) {
            ZK_CLUSTER.shutdown();
        }
        FileUtils.deleteDirectory(ZK_DIR);
    }

    private static int getFreePort() {
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(0);
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new RuntimeException("Error finding a free port", e);
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException("Error closing ServerSocket used to detect a free port.", e);
                }
            }
        }
    }
    
    private void addAndWaitForIndexer(IndexerDefinition indexerDef) throws Exception {
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

    @Before
    public void setUp() throws /*ZkConnectException,*/ InterruptedException, /*KeeperException,*/ IOException {
        conf = new Configuration();
        opts = new HBaseIndexingOptions(conf);

        HBaseAdmin hBaseAdmin = Mockito.mock(HBaseAdmin.class);
        Mockito.when(hBaseAdmin.listTables("record")).thenReturn(new HTableDescriptor[]{
                new HTableDescriptor("record")
        });
        opts.hBaseAdmin = hBaseAdmin;
    }
    
    @Test
    public void testIsDirectWrite_True() {
        opts.reducers = 0;

        assertTrue(opts.isDirectWrite());
    }


    @Test
    public void testIsDirectWrite_False() {
        opts.reducers = 1;
        assertFalse(opts.isDirectWrite());

        // Different negative values have different meanings in core search MR job
        opts.reducers = -1;
        assertFalse(opts.isDirectWrite());

        opts.reducers = -2;
        assertFalse(opts.isDirectWrite());
    }

    @Test
    public void testEvaluateOutputDir_DirectWrite() {
        opts.outputDir = null;
        opts.reducers = 0;
        opts.zkHost = "myzkhost/solr";

        // Should have no effect
        opts.evaluateOutputDir();

        assertNull(opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }

    @Test
    public void testEvaluateOutputDir_DryRun() {
        opts.outputDir = null;
        opts.isDryRun = true;
        opts.zkHost = "myzkhost/solr";

        // Should have no effect
        opts.evaluateOutputDir();

        assertNull(opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateOutputDir_DirectWrite_NoSolrZkHostDefined() {
        opts.outputDir = null;
        opts.reducers = 0;
        opts.zkHost = null;

        opts.evaluateOutputDir();
    }
    
    @Test
    public void testEvaluateOutputDir_DirectWrite_WithZkBasedIndexer() {
        opts.outputDir = null;
        opts.reducers = 0;
        opts.zkHost = null;
        opts.hbaseIndexerName = "indexer";
        opts.hbaseIndexerZkHost = "indexerhost";

        // Should have no effect
        opts.evaluateOutputDir();

        assertNull(opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }
    
    @Test
    public void testEvaluateOutputDir_GoLive() {
        opts.outputDir = null;
        opts.reducers = 2;
        opts.goLive = true;

        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/tmp"), outputPath.getParent());
        assertTrue(opts.isGeneratedOutputDir());
    }

    @Test
    public void testEvaluateOutputDir_GoLive_UserDefinedOutputDir() {
        opts.outputDir = new Path("/outputdir");
        opts.reducers = 2;
        opts.goLive = true;

        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/outputdir"), outputPath);
        assertFalse(opts.isGeneratedOutputDir());
    }

    @Test
    public void testEvaluateOutputDir_GoLive_AlternateTempDirViaConfig() {
        opts.outputDir = null;
        opts.reducers = 2;
        opts.goLive = true;
        conf.set("hbase.search.mr.tmpdir", "/othertmp");

        opts.evaluateOutputDir();

        Path outputPath = opts.outputDir;
        assertEquals(new Path("/othertmp"), outputPath.getParent());
        assertTrue(opts.isGeneratedOutputDir());
    }

    @Test
    public void testEvaluateOutputDir_WriteToFile() {
        opts.outputDir = new Path("/output/path");
        opts.reducers = 2;

        // Should have no effect
        opts.evaluateOutputDir();

        assertEquals(new Path("/output/path"), opts.outputDir);
        assertFalse(opts.isGeneratedOutputDir());
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateOutputDir_NoOutputDirButNotDirectWriteMode() {
        opts.outputDir = null;
        opts.reducers = 1;

        opts.evaluateOutputDir();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateOutputDir_OutputDirSetAlongWithDirectWriteMode() {
        opts.outputDir = new Path("/some/path");
        opts.reducers = 0;

        opts.evaluateOutputDir();
    }
    
    @Test
    public void testEvaluateIndexingSpecification_PullSolrZkHostFromIndexerDefinition() throws Exception {
        
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("customsolr")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        
        addAndWaitForIndexer(indexerDef);
        
        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = "customsolr";
        opts.zkHost = "customhost/solr";
        opts.collection = null;
        
        opts.evaluateIndexingSpecification();
        
        INDEXER_MODEL.deleteIndexerInternal("customsolr");
        
        assertEquals("customhost/solr", opts.zkHost);
        assertEquals("mycollection", opts.collection);
    }
    
    @Test
    public void testIndexingSpecification_PullSolrCollectionFromIndexerDefinition() throws Exception {

        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("customcollection")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        
        addAndWaitForIndexer(indexerDef);
        
        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = "customcollection";
        opts.zkHost = null;
        opts.collection = "customcollection";
        
        opts.evaluateIndexingSpecification();
        
        INDEXER_MODEL.deleteIndexerInternal("customcollection");
        
        assertEquals("myZkHost/solr", opts.zkHost);
        assertEquals("customcollection", opts.collection);
    }

    @Test
    public void testEvaluateScan_StartRowDefined() throws Exception {

        // Set up the dependencies for the indexing specification
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "mock_indexer.xml").toURI());
        opts.zkHost = "myzkhost";
        opts.collection = "mycollection";
        opts.evaluateIndexingSpecification();

        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.hbaseStartRow = "starthere";
        opts.evaluateScan();
        assertArrayEquals(Bytes.toBytes("starthere"), opts.getScans().get(0).getStartRow());
    }

    @Test
    public void testEvaluateScan_EndRowDefined() throws Exception {

        // Set up the dependencies for the indexing specification
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "mock_indexer.xml").toURI());
        opts.zkHost = "myzkhost";
        opts.collection = "mycollection";
        opts.evaluateIndexingSpecification();

        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.hbaseEndRow = "endhere";
        opts.evaluateScan();
        assertArrayEquals(Bytes.toBytes("endhere"), opts.getScans().get(0).getStopRow());
    }

    @Test
    public void testEvaluateScan_StartTimeDefined() throws Exception {

        // Set up the dependencies for the indexing specification
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "mock_indexer.xml").toURI());
        opts.zkHost = "myzkhost";
        opts.collection = "mycollection";
        opts.evaluateIndexingSpecification();

        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.hbaseStartTimeString = "220777";
        opts.evaluateScan();
        assertEquals(220777L, opts.getScans().get(0).getTimeRange().getMin());
    }

    @Test
    public void testEvaluateScan_EndTimeDefined() throws Exception {

        // Set up the dependencies for the indexing specification
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "mock_indexer.xml").toURI());
        opts.zkHost = "myzkhost";
        opts.collection = "mycollection";
        opts.evaluateIndexingSpecification();

        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.hbaseEndTimeString = "220777";
        opts.evaluateScan();
        assertEquals(220777L, opts.getScans().get(0).getTimeRange().getMax());
    }

    @Test
    public void testEvaluateScan_CheckFamilyMap() throws Exception{
        // Check that the Scan only scans values referred to via the
        // ResultToSolrMapper#getGet(byte[]) method

        // Set up the dependencies for the indexing specification
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "mock_indexer.xml").toURI());
        opts.zkHost = "myzkhost";
        opts.collection = "mycollection";
        opts.evaluateIndexingSpecification();


        opts.evaluateScan();

        Scan scan = opts.getScans().get(0);
        assertTrue(scan.getFamilyMap().containsKey(Bytes.toBytes("info")));
        // Should be firstname, lastname, age
        assertEquals(3, scan.getFamilyMap().get(Bytes.toBytes("info")).size());
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_NoZkHostOrSolrHomeDir() {
        opts.solrHomeDir = null;
        opts.zkHost = null;

        opts.evaluateGoLiveArgs();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_GoLiveButNoZkAndNoShards() {
        opts.goLive = true;
        opts.zkHost = null;
        opts.shardUrls = null;

        opts.evaluateGoLiveArgs();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_ZkHostButNoCollection() {
        opts.zkHost = "myzkhost";
        opts.collection = null;

        opts.evaluateGoLiveArgs();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_EmptyShardUrls() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shardUrls = ImmutableList.of();

        opts.evaluateGoLiveArgs();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateGoLiveArgs_ZeroShards() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shards = 0;

        opts.evaluateGoLiveArgs();
    }

    @Test
    public void testEvaluateGoLiveArgs_AutoSetShardCount() {
        opts.solrHomeDir = new File("/solrhome");
        opts.shardUrls = ImmutableList.<List<String>>of(ImmutableList.of("shard_a"), ImmutableList.of("shard_b"));
        opts.shards = null;

        opts.evaluateGoLiveArgs();

        assertEquals(2, (int)opts.shards);

    }


    @Test
    public void testEvaluateIndexingSpecification_AllFromZooKeeper() throws Exception {

        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("userindexer")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        
        addAndWaitForIndexer(indexerDef);

        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = "userindexer";

        opts.evaluateIndexingSpecification();
        INDEXER_MODEL.deleteIndexerInternal("userindexer");
        
        IndexingSpecification expectedSpec = new IndexingSpecification(
                            "record", "userindexer", null,
                Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")), ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"));




        assertEquals(expectedSpec, opts.getIndexingSpecification());
    }

    @Test
    public void testEvaluateIndexingSpecification_AllFromCmdline() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";

        opts.evaluateIndexingSpecification();

        IndexingSpecification expectedSpec = new IndexingSpecification(
                            "mytable", HBaseIndexingOptions.DEFAULT_INDEXER_NAME, DefaultIndexerComponentFactory.class.getName(),
                Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")), ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"));

        assertEquals(expectedSpec, opts.getIndexingSpecification());

    }

    @Test
    public void testEvaluateIndexingSpecification_SolrClassic() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = null;
        opts.collection = null;
        opts.solrHomeDir = new File(".");

        opts.evaluateIndexingSpecification();

        IndexingSpecification expectedSpec = new IndexingSpecification(
                            "mytable", HBaseIndexingOptions.DEFAULT_INDEXER_NAME, DefaultIndexerComponentFactory.class.getName(),
                Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")), ImmutableMap.of(
                        "solr.mode", "classic",
                        "solr.home", opts.solrHomeDir.getAbsolutePath()));

        assertEquals(expectedSpec, opts.getIndexingSpecification());
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_IndexerZkSuppliedButNoIndexerNameSupplied() throws Exception {
        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = null;

        opts.evaluateIndexingSpecification();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NonExistantIndexerSupplied() throws Exception {

        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = "NONEXISTANT";

        opts.evaluateIndexingSpecification();
    }

    @Test
    public void testEvaluateIndexingSpecification_TableNameFromXmlFile() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";

        opts.evaluateIndexingSpecification();

        IndexingSpecification expectedSpec = new IndexingSpecification(
                "record", HBaseIndexingOptions.DEFAULT_INDEXER_NAME, DefaultIndexerComponentFactory.class.getName(),
                Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")), ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"));

        assertEquals(expectedSpec, opts.getIndexingSpecification());
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoIndexXmlSpecified() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.hbaseIndexerConfigFile = null;
        opts.hbaseTableName = "mytable";
        opts.zkHost = "myZkHost/solr";
        opts.collection = "mycollection";

        opts.evaluateIndexingSpecification();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoZkHostSpecified() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.zkHost = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.collection = "mycollection";

        opts.evaluateIndexingSpecification();
    }

    @Test(expected=IllegalStateException.class)
    public void testEvaluateIndexingSpecification_NoCollectionSpecified() throws Exception {
        opts.hbaseIndexerZkHost = null;
        opts.collection = null;
        opts.hbaseTableName = "mytable";
        opts.hbaseIndexerConfigFile = new File(Resources.getResource(getClass(), "user_indexer.xml").toURI());
        opts.zkHost = "myZkHost/solr";

        opts.evaluateIndexingSpecification();
    }

    @Test
    public void testEvaluateIndexingSpecification_CombinationOfCmdlineAndZk() throws Exception {

        // Create an indexer -- verify INDEXER_ADDED event
        IndexerDefinition indexerDef = new IndexerDefinitionBuilder()
                .name("userindexer")
                .configuration(Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")))
                .connectionParams(ImmutableMap.of(
                        "solr.zk", "myZkHost/solr",
                        "solr.collection", "mycollection"))
                .build();
        addAndWaitForIndexer(indexerDef);

        opts.hbaseIndexerZkHost = "localhost:" + ZK_CLIENT_PORT;
        opts.hbaseIndexerName = "userindexer";
        opts.hbaseTableName = "mytable";
        opts.zkHost = "myOtherZkHost/solr";

        opts.evaluateIndexingSpecification();

        INDEXER_MODEL.deleteIndexerInternal("userindexer");

        IndexingSpecification expectedSpec = new IndexingSpecification(
                "mytable", "userindexer", null,
                Resources.toByteArray(Resources.getResource(getClass(), "user_indexer.xml")), ImmutableMap.of(
                        "solr.zk", "myOtherZkHost/solr",
                        "solr.collection", "mycollection"));

        assertEquals(expectedSpec, opts.getIndexingSpecification());

    }
    
    @Test
    public void testEvaluateTimestamp_NoFormatSupplied() {
        assertEquals(
                Long.valueOf(12345),
                HBaseIndexingOptions.evaluateTimestamp("12345", null));
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateTimestamp_NoFormat_NonParseableLong() {
        HBaseIndexingOptions.evaluateTimestamp("abc", null);
    }
    
    @Test
    public void testEvaluateTimestamp_CustomTimestampFormat() {
        assertEquals(
                Long.valueOf(DateTimeFormat.forPattern("yyyy/MM/dd HH:mm:ss").parseMillis("2013/10/20 00:39:00")),
                HBaseIndexingOptions.evaluateTimestamp("2013/10/20 00:39:00", "yyyy/MM/dd HH:mm:ss"));
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateTimestamp_InvalidTimestampFormat() {
        HBaseIndexingOptions.evaluateTimestamp("2013/10/20 00:39", "not a timestamp format");
    }
    
    @Test(expected=IllegalStateException.class)
    public void testEvaluateTimestamp_TimestampNotAccordingToFormat() {
        HBaseIndexingOptions.evaluateTimestamp("invalid timestamp data", "yyyy/MM/dd HH:mm");
    }
    
    @Test
    public void testEvaluateTimestamp_NullTimestamp() {
        assertNull(HBaseIndexingOptions.evaluateTimestamp(null, null));
    }

    @Test
    public void testHelp() throws Exception {
      String[] args = new String[] {"--help"};
      assertEquals(0, ToolRunner.run(new Configuration(), new HBaseMapReduceIndexerTool(), args));
    }

    @Test
    public void testHelpWithNonSolrCloud() throws Exception {
      String[] args = new String[] {"--help", "--show-non-solr-cloud"};
      assertEquals(0, ToolRunner.run(new Configuration(), new HBaseMapReduceIndexerTool(), args));
    }

}
