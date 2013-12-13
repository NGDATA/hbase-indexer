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

import java.util.Map;

import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Pipes a given HBase Result into a morphline and extracts and transforms the specified HBase cells to a
 * SolrInputDocument. Note that for proper functioning the morphline should not contain a loadSolr command because
 * loading documents into Solr is the responsibility of the enclosing Indexer.
 * 
 * Example config file:
 * <pre>
 * {@code
 * <indexer>
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
 * }
 * </pre>
 * <p>
 * This class is actually a thread-safe wrapper around the {@link LocalMorphlineResultToSolrMapper}, which handles actual
 * morphline execution.
 * 
 * @see LocalMorphlineResultToSolrMapper
 */
public final class MorphlineResultToSolrMapper implements ResultToSolrMapper, Configurable {

    private Map<String, String> params;
    
    /*
     * The SEP calls the *same* MorphlineResultToSolrMapper instance from multiple threads at the same
     * time. Morphlines contain state within a method call, so we use a single LocalMorphlineResultToSolrMapper
     * per thread.
     */
    private final ThreadLocal<LocalMorphlineResultToSolrMapper> localMorphlineMapper = new ThreadLocal<LocalMorphlineResultToSolrMapper>() {
        
        @Override
        protected LocalMorphlineResultToSolrMapper initialValue() {
            if (params == null) {
                throw new IllegalStateException("Can't create a LocalMorphlineToSolrMapper, not yet configured");
            }
            LocalMorphlineResultToSolrMapper localMorphlineMapper = new LocalMorphlineResultToSolrMapper();
            localMorphlineMapper.configure(params);
            return localMorphlineMapper;
        }
    };

    public static final String MORPHLINE_FILE_PARAM = "morphlineFile";
    public static final String MORPHLINE_ID_PARAM = "morphlineId";

    /**
     * Morphline variables can be passed from the CLI or indexer definition config file to the Morphline, e.g.: 
     * hadoop ... -D morphlineVariable.zkHost=127.0.0.1:2181/solr
     * or
     * <param name="morphlineVariable.zkHost" value="127.0.0.1:2181/solr"/>
     */
    public static final String MORPHLINE_VARIABLE_PARAM = "morphlineVariable";

    /**
     * Record fields can be passed from the CLI or indexer definition config file to the Morphline, e.g.
     * hadoop ... -D morphlineField._attachment_mimetype=text/csv
     * or
     * <param name="morphlineField._attachment_mimetype" value="text/csv"/>
     */
    public static final String MORPHLINE_FIELD_PARAM = "morphlineField";
    
    public static final String OUTPUT_MIME_TYPE = "application/java-hbase-result";

    public MorphlineResultToSolrMapper() {
    }

    @Override
    public void configure(Map<String, String> config) {
        Preconditions.checkNotNull(config);
        this.params = config;
        this.localMorphlineMapper.get(); // startup: fail fast on morphline compilation error
    }

    @Override
    public boolean containsRequiredData(Result result) {
        return localMorphlineMapper.get().containsRequiredData(result);
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        return localMorphlineMapper.get().isRelevantKV(kv);
    }

    @Override
    public Get getGet(byte[] row) {
        return localMorphlineMapper.get().getGet(row);
    }

    @Override
    public void map(Result result, SolrUpdateWriter solrUpdateWriter) {
        localMorphlineMapper.get().map(result, solrUpdateWriter);
    }

}
