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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;

/**
 * Instantiates and configures {@code ResultToSolrMapper} instances based on a supplied hbase-indexer configuration.
 */
public class ResultToSolrMapperFactory {

    private static final Log LOG = LogFactory.getLog(ResultToSolrMapperFactory.class);

    private static final String SOLR_HOME_PROPERTY_NAME = "solr.solr.home";

    /**
     * Instantiate a ResultToSolrMapper based on a configuration supplied through an input stream.
     *
     * @param indexName name of the index for which the mapper is to be created
     * @param indexerConf configuration containing the index definition
     * @return configured ResultToSolrMapper
     */
    public static ResultToSolrMapper createResultToSolrMapper(String indexName, IndexerConf indexerConf) {

        ResultToSolrMapper mapper = null;
        try {
            if (indexerConf.getMapperClass().equals(DefaultResultToSolrMapper.class)) {
                // FIXME: this is cheating. Knowledge about mapper implementations should be handled by IndexerComponentFactory
                mapper = new DefaultResultToSolrMapper(indexName, indexerConf.getFieldDefinitions(),
                        indexerConf.getDocumentExtractDefinitions());
            } else {
                mapper = indexerConf.getMapperClass().newInstance();
                ConfigureUtil.configure(mapper, indexerConf.getGlobalParams());
            }
        } catch (Exception e) {
            LOG.error("Error instantiating ResultToSolrMapper for " + indexName, e);
            throw new RuntimeException(e);
        }
        return mapper;

    }

}
