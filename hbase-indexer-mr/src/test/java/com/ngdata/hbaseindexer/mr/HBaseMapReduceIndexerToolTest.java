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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class HBaseMapReduceIndexerToolTest {

    private static final byte[] TEST_TABLE_NAME = Bytes.toBytes("record");
    private static final byte[] TEST_COLFAM_NAME = Bytes.toBytes("info");
    
    private static final HBaseTestingUtility HBASE_TEST_UTILITY = HBaseTestingUtilityFactory.createTestUtility();
    private static MRTestUtil MR_TEST_UTIL;
    
    private static final String RESOURCES_DIR = "target/test-classes";
    private static final File MINIMR_CONF_DIR = new File(RESOURCES_DIR + "/solr/minimr");
    
    private static final int RECORD_COUNT = 2000;
    
    
    private static HBaseAdmin HBASE_ADMIN;

    private static HTable RECORD_TABLE;
    
    
    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        MR_TEST_UTIL = new MRTestUtil(HBASE_TEST_UTILITY);
        HBASE_TEST_UTILITY.startMiniCluster();
        MR_TEST_UTIL.startMrCluster();
        
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
        HBASE_ADMIN.close();
        HBASE_TEST_UTILITY.shutdownMiniMapReduceCluster();
        HBASE_TEST_UTILITY.shutdownMiniCluster();
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
    
    @Test
    public void testIndexer_SingleShard() throws Exception {
        
        FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
            "--solr-home-dir", MINIMR_CONF_DIR.toString(),
            "--output-dir", fs.makeQualified(new Path("/solroutput")).toString(),
            "--shards", "1",
            "--overwrite-output-dir");
        
        TestUtils.validateSolrServerDocumentCount(
                MINIMR_CONF_DIR,
                FileSystem.get(HBASE_TEST_UTILITY.getConfiguration()),
                new Path("/solroutput", "results"),
                RECORD_COUNT,
                1);
            
    }
    
    @Test
    public void testIndexer_MultipleShards() throws Exception {
        
        FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
            "--solr-home-dir", MINIMR_CONF_DIR.toString(),
            "--output-dir", fs.makeQualified(new Path("/solroutput")).toString(),
            "--shards", "3",
            "--overwrite-output-dir");

        TestUtils.validateSolrServerDocumentCount(
                MINIMR_CONF_DIR,
                FileSystem.get(HBASE_TEST_UTILITY.getConfiguration()),
                new Path("/solroutput", "results"),
                RECORD_COUNT,
                3);
            
    }
    
    @Test
    public void testIndexer_Morphlines() throws Exception {
        
        FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", new File(Resources.getResource("morphline_indexer_without_zk.xml").toURI()).toString(),
            "--solr-home-dir", MINIMR_CONF_DIR.toString(),
            "--output-dir", fs.makeQualified(new Path("/solroutput")).toString(),
            "--shards", "2",
            "--reducers", "8",
            "--fanout", "2",
            "--morphline-file", new File(Resources.getResource("extractHBaseCellWithoutZk.conf").toURI()).toString(),
            "--overwrite-output-dir",
            "--hbase-table-name", "record",
            "--verbose",
                "--log4j", new File(Resources.getResource("log4j.properties").toURI()).toString()
            );

        TestUtils.validateSolrServerDocumentCount(
                MINIMR_CONF_DIR,
                FileSystem.get(HBASE_TEST_UTILITY.getConfiguration()),
                new Path("/solroutput", "results"),
                RECORD_COUNT,
                2);
            
    }
    
    @Test
    public void testIndexer_StartAndEndRows() throws Exception {
        
        FileSystem fs = FileSystem.get(HBASE_TEST_UTILITY.getConfiguration());
        MR_TEST_UTIL.runTool(
            "--hbase-indexer-file", new File(Resources.getResource(getClass(), "user_indexer.xml").toURI()).toString(),
            "--solr-home-dir", MINIMR_CONF_DIR.toString(),
            "--output-dir", fs.makeQualified(new Path("/solroutput")).toString(),
            "--shards", "1",
            "--hbase-start-row", "row0100",
            "--hbase-end-row", "row1000",
            "--max-segments", "2",
            "--overwrite-output-dir");

        TestUtils.validateSolrServerDocumentCount(
                MINIMR_CONF_DIR,
                FileSystem.get(HBASE_TEST_UTILITY.getConfiguration()),
                new Path("/solroutput", "results"),
                900,
                1);
            
    }
    
    

}
