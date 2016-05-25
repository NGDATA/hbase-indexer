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
package com.ngdata.hbaseindexer.indexer;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;

import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.schema.IndexSchema;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.util.net.NetUtils;
import com.ngdata.hbaseindexer.util.solr.SolrTestingUtility;

public class ResultToSolrMapperFactoryTest {

    private static MiniZooKeeperCluster ZK_CLUSTER;
    private static File ZK_DIR;
    private static int ZK_CLIENT_PORT;

    private static SolrTestingUtility SOLR_TEST_UTILITY;
    private static CloudSolrClient COLLECTION1;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        ZK_DIR = new File(System.getProperty("java.io.tmpdir") + File.separator + "resulttosolrmapperfactory.zktest");
        ZK_CLIENT_PORT = getFreePort();

        ZK_CLUSTER = new MiniZooKeeperCluster();
        ZK_CLUSTER.setDefaultClientPort(ZK_CLIENT_PORT);
        ZK_CLUSTER.startup(ZK_DIR);

        SOLR_TEST_UTILITY = new SolrTestingUtility(ZK_CLIENT_PORT, NetUtils.getFreePort());
        SOLR_TEST_UTILITY.start();
        SOLR_TEST_UTILITY.uploadConfig("config1",
                Resources.toByteArray(Resources.getResource(ResultToSolrMapperFactoryTest.class, "schema.xml")),
                Resources.toByteArray(Resources.getResource(ResultToSolrMapperFactoryTest.class, "solrconfig.xml")));
        SOLR_TEST_UTILITY.createCore("collection1_core1", "collection1", "config1", 1);

        COLLECTION1 = new CloudSolrClient(SOLR_TEST_UTILITY.getZkConnectString());
        COLLECTION1.setDefaultCollection("collection1");
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        SOLR_TEST_UTILITY.stop();
        ZK_CLUSTER.shutdown();
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

}
