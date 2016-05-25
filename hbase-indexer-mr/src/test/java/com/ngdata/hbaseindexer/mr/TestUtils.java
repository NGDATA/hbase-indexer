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
package com.ngdata.hbaseindexer.mr;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.HdfsDirectoryFactory;
import org.apache.solr.core.SolrCore;
import org.apache.solr.core.SolrResourceLoader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Locale;

import static org.junit.Assert.assertEquals;


public class TestUtils {

  private static final Log LOG = LogFactory.getLog(TestUtils.class);

  public static void validateSolrServerDocumentCount(File solrHomeDir, FileSystem fs, Path outDir, int expectedDocs, int expectedShards)
      throws IOException, SolrServerException {
    
    long actualDocs = 0;
    int actualShards = 0;
    for (FileStatus dir : fs.listStatus(outDir)) { // for each shard
      if (dir.getPath().getName().startsWith("part") && dir.isDirectory()) {
        actualShards++;
        EmbeddedSolrServer solr = createEmbeddedSolrServer(
            new Path(solrHomeDir.getAbsolutePath()), fs, dir.getPath());
        
        try {
          SolrQuery query = new SolrQuery();
          query.setQuery("*:*");
          QueryResponse resp = solr.query(query);
          long numDocs = resp.getResults().getNumFound();
          actualDocs += numDocs;
        } finally {
          solr.shutdown();
        }
      }
    }
    assertEquals(expectedShards, actualShards);
    assertEquals(expectedDocs, actualDocs);
  }

  public static EmbeddedSolrServer createEmbeddedSolrServer(Path solrHomeDir, FileSystem fs, Path outputShardDir)
          throws IOException {

    LOG.info("Creating embedded Solr server with solrHomeDir: " + solrHomeDir + ", fs: " + fs + ", outputShardDir: " + outputShardDir);

    Path solrDataDir = new Path(outputShardDir, "data");

    String dataDirStr = solrDataDir.toUri().toString();

    SolrResourceLoader loader = new SolrResourceLoader(Paths.get(solrHomeDir.toString()), null, null);

    LOG.info(String
            .format(Locale.ENGLISH,
                    "Constructed instance information solr.home %s (%s), instance dir %s, conf dir %s, writing index to solr.data.dir %s, with permdir %s",
                    solrHomeDir, solrHomeDir.toUri(), loader.getInstancePath(),
                    loader.getConfigDir(), dataDirStr, outputShardDir));

    // TODO: This is fragile and should be well documented
    System.setProperty("solr.directoryFactory", HdfsDirectoryFactory.class.getName());
    System.setProperty("solr.lock.type", DirectoryFactory.LOCK_TYPE_HDFS);
    System.setProperty("solr.hdfs.nrtcachingdirectory", "false");
    System.setProperty("solr.hdfs.blockcache.enabled", "false");
    System.setProperty("solr.autoCommit.maxTime", "600000");
    System.setProperty("solr.autoSoftCommit.maxTime", "-1");

    CoreContainer container = new CoreContainer(loader);
    container.load();

    SolrCore core = container.create("core1", ImmutableMap.of(CoreDescriptor.CORE_DATADIR, dataDirStr));

    if (!(core.getDirectoryFactory() instanceof HdfsDirectoryFactory)) {
      throw new UnsupportedOperationException(
              "Invalid configuration. Currently, the only DirectoryFactory supported is "
                      + HdfsDirectoryFactory.class.getSimpleName());
    }

    EmbeddedSolrServer solr = new EmbeddedSolrServer(container, "core1");
    return solr;
  }

}
