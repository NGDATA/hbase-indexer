/*
 * Copyright 2013 Cloudera Inc.
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
package com.ngdata.hbaseindexer.morphline;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;

import com.google.common.base.Preconditions;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;

/**
 * Pipes a given HBase Result into a morphline and extracts and transforms the specified HBase cells
 * to a SolrInputDocument. Note that for proper functioning the morphline should not contain a
 * loadSolr command because loading documents into Solr is the responsibility of the enclosing Indexer.
 * 
 * Example config file:
 * 
 * <pre>
 * <indexer 
 * 
 *   <!--
 *   The HBase Lily Morphline Indexer supports the standard attributes of an HBase Lily Indexer 
 *   (i.e. table, mapping-type, read-row, unique-key-formatter, unique-key-field, row-field, column-family-field), 
 *   as documented at https://github.com/NGDATA/hbase-indexer/wiki/Indexer-configuration
 *
 *   In addition, morphline specific attributes are supported, as follows:
 *   --> 
 * 
 *   <!-- The name of the HBase table to index -->
 *   table="record" 
 *   
 *   <!-- Parameter mapper (required): Fully qualified class name of morphline mapper -->
 *   mapper="com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper">
 *   
 *   <!--
 *   Parameter morphlineFile (required): The relative or absolute path on the local file system to the morphline configuration file. Example: /etc/hbase-solr/conf/morphlines.conf
 *   -->
 *   <param name="morphlineFile" value="morphlines.conf"/>
 *   
 *   <-- 
 *   Parameter morphlineId (optional): Name used to identify a morphline if there are multiple morphlines in a morphline config file 
 *   -->
 *   <param name="morphlineId" value="morphline1"/>
 *   
 * </indexer>
 * </pre>
 */
public final class MorphlineResultToSolrMapper implements ResultToSolrMapper, Configurable {
    
  private Map<String, String> params;
  private final Object lock = new Object();
  
  /*
   * TODO: Looks like SEP calls the *same* MorphlineResultToSolrMapper instance from multiple
   * threads at the same time. This would cause race conditions. It would be more efficient,
   * scalable and simple for SEP to instantiate a separate ResultToSolrMapper instance per thread on
   * program startup (and have one or two threads per CPU core), and have each thread reuse it's own
   * local ResultToSolrMapper many times. For now we use a crude lower-level pooling work-around
   * below.
   */
  private final BlockingQueue<LocalMorphlineResultToSolrMapper> pool = new LinkedBlockingQueue();

  public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
  public static final String MORPHLINE_ID_PARAM = "morphlineId";

  /**
   * Morphline variables can be passed from the indexer definition config file to the Morphline, e.g.:
   * <param name="morphlineVariable.zkHost" value="127.0.0.1:2181/solr"/>
   */
  public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";

  public static final String OUTPUT_MIME_TYPE = "application/java-hbase-result";
  
  public MorphlineResultToSolrMapper() {}
  
  @Override
  public void configure(Map<String, String> params) {
    Preconditions.checkNotNull(params);
    this.params = params;
    returnToPool(borrowFromPool()); // fail fast on morphline compilation exception
  }
  
  private LocalMorphlineResultToSolrMapper borrowFromPool() {
    LocalMorphlineResultToSolrMapper mapper = pool.poll();
    if (mapper == null) {
      mapper = createMapper();
    }
    return mapper;
  }

  private LocalMorphlineResultToSolrMapper createMapper() {
    LocalMorphlineResultToSolrMapper mapper = new LocalMorphlineResultToSolrMapper();
    Map<String, String> localParams;
    synchronized (lock) {
      localParams = new HashMap(params);
    }
    mapper.configure(localParams);
    return mapper;
  }

  private void returnToPool(LocalMorphlineResultToSolrMapper mapper) {
    try {
      pool.put(mapper);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public boolean containsRequiredData(Result result) {
    LocalMorphlineResultToSolrMapper mapper = borrowFromPool();
    boolean returnValue = mapper.containsRequiredData(result);
    returnToPool(mapper);
    return returnValue;
  }

  @Override
  public boolean isRelevantKV(KeyValue kv) {
    LocalMorphlineResultToSolrMapper mapper = borrowFromPool();
    boolean returnValue = mapper.isRelevantKV(kv);
    returnToPool(mapper);
    return returnValue;
  }

  @Override
  public Get getGet(byte[] row) {
    LocalMorphlineResultToSolrMapper mapper = borrowFromPool();
    Get returnValue = mapper.getGet(row);
    returnToPool(mapper);
    return returnValue;
  }

  @Override
  public SolrInputDocument map(Result result) {
    LocalMorphlineResultToSolrMapper mapper = borrowFromPool();
    SolrInputDocument returnValue = mapper.map(result);
    returnToPool(mapper);
    return returnValue;
  }

}
