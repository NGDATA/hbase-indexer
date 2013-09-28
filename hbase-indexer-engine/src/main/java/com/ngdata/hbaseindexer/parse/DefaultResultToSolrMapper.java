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
package com.ngdata.hbaseindexer.parse;

import static com.ngdata.sep.impl.HBaseShims.newGet;

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.concurrent.TimeUnit;

import com.ngdata.hbaseindexer.ConfigureUtil;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.DocumentExtractDefinition;
import com.ngdata.hbaseindexer.conf.FieldDefinition;
import com.ngdata.hbaseindexer.parse.extract.ByteArrayExtractors;
import com.ngdata.hbaseindexer.parse.tika.TikaSolrDocumentExtractor;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.schema.IndexSchema;

/**
 * Parses HBase {@code Result} objects into a structure of fields and values.
 */
public class DefaultResultToSolrMapper implements ResultToSolrMapper {
    
    /**
     * Map of Solr field names to transformers for extracting data from HBase {@code Result} objects.
     */
    private List<SolrDocumentExtractor> resultDocumentExtractors;

    /**
     * Information to be used for constructing a Get to fetch data required for indexing.
     */
    private Map<byte[], NavigableSet<byte[]>> familyMap;

    /**
     * Used to do evaluation on applicability of KeyValues.
     */
    private List<ByteArrayExtractor> extractors;
    
    private Timer mappingTimer;
    
    /**
     * Instantiate with {@code FieldDefinitions}s and {@code DocumentExtractDefinition}s.
     * 
     * @param fieldDefinitions define fields to be indexed
     * @param documentExtractDefinitions additional document extraction definitions
     * @param indexSchema Solr index schema for the target index
     */
    public DefaultResultToSolrMapper(String indexerName, List<FieldDefinition> fieldDefinitions,
            List<DocumentExtractDefinition> documentExtractDefinitions, IndexSchema indexSchema) {
        extractors = Lists.newArrayList();
        resultDocumentExtractors = Lists.newArrayList();
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    fieldDefinition.getValueExpression(), fieldDefinition.getValueSource());
            ByteArrayValueMapper valueMapper = ByteArrayValueMappers.getMapper(fieldDefinition.getTypeName());
            ConfigureUtil.configure(valueMapper, fieldDefinition.getParams());
            resultDocumentExtractors.add(new HBaseSolrDocumentExtractor(fieldDefinition.getName(), byteArrayExtractor,
                    valueMapper));
            extractors.add(byteArrayExtractor);
        }

        for (DocumentExtractDefinition extractDefinition : documentExtractDefinitions) {
            ByteArrayExtractor byteArrayExtractor = ByteArrayExtractors.getExtractor(
                    extractDefinition.getValueExpression(), extractDefinition.getValueSource());
            
            TikaSolrDocumentExtractor tikaDocumentExtractor = new TikaSolrDocumentExtractor(
                    indexSchema, byteArrayExtractor, extractDefinition.getPrefix(), extractDefinition.getMimeType());
            ConfigureUtil.configure(tikaDocumentExtractor, extractDefinition.getParams());
            resultDocumentExtractors.add(tikaDocumentExtractor);
            extractors.add(byteArrayExtractor);
        }

        Get get = newGet();
        for (ByteArrayExtractor extractor : extractors) {

            byte[] columnFamily = extractor.getColumnFamily();
            byte[] columnQualifier = extractor.getColumnQualifier();
            if (columnFamily != null) {
                if (columnQualifier != null) {
                    get.addColumn(columnFamily, columnQualifier);
                } else {
                    get.addFamily(columnFamily);
                }
            }
        }
        familyMap = get.getFamilyMap();
        
        mappingTimer = Metrics.newTimer(metricName(getClass(), "HBase Result to Solr mapping time", indexerName),
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    }
    
    @Override
    public boolean containsRequiredData(Result result) {
        for (ByteArrayExtractor extractor : extractors) {
            if (!extractor.containsTarget(result)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        for (ByteArrayExtractor extractor : extractors) {
            if (extractor.isApplicable(kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Get getGet(byte[] row) {
        Get get = new Get(row);
        for (Entry<byte[], NavigableSet<byte[]>> familyMapEntry : familyMap.entrySet()) {
            byte[] columnFamily = familyMapEntry.getKey();
            if (familyMapEntry.getValue() == null) {
                get.addFamily(columnFamily);
            } else {
                for (byte[] qualifier : familyMapEntry.getValue()) {
                    get.addColumn(columnFamily, qualifier);
                }
            }
        }
        return get;
    }

    @Override
    public SolrInputDocument map(Result result) {
        TimerContext timerContext = mappingTimer.time();
        try {
            SolrInputDocument solrInputDocument = new SolrInputDocument();
            for (SolrDocumentExtractor documentExtractor : resultDocumentExtractors) {
                documentExtractor.extractDocument(result, solrInputDocument);
            }
            return solrInputDocument;
        } finally {
            timerContext.stop();
        }
    }

}
