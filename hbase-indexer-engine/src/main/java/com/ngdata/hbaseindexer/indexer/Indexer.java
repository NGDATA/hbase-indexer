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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Table;
import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import com.ngdata.hbaseindexer.uniquekey.UniqueTableKeyFormatter;
import com.ngdata.sep.util.io.Closer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

/**
 * The indexing algorithm. It receives an event from the SEP, handles it based on the configuration, and eventually
 * calls Solr.
 */
public abstract class Indexer {

    protected Log log = LogFactory.getLog(getClass());

    private String indexerName;
    protected IndexerConf conf;
    protected final String tableName;
    private Sharder sharder;
    private SolrInputDocumentWriter solrWriter;
    protected ResultToSolrMapper mapper;
    protected UniqueKeyFormatter uniqueKeyFormatter;
    private Timer indexingTimer;


    /**
     * Instantiate an indexer based on the given {@link IndexerConf}.
     */
    public static Indexer createIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper,
                                        HTablePool tablePool, Sharder sharder, SolrInputDocumentWriter solrWriter) {
        switch (conf.getMappingType()) {
            case COLUMN:
                return new ColumnBasedIndexer(indexerName, conf, tableName, mapper, sharder, solrWriter);
            case ROW:
                return new RowBasedIndexer(indexerName, conf, tableName, mapper, tablePool, sharder, solrWriter);
            default:
                throw new IllegalStateException("Can't determine the type of indexing to use for mapping type "
                        + conf.getMappingType());
        }
    }

    Indexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper, Sharder sharder,
            SolrInputDocumentWriter solrWriter) {
        this.indexerName = indexerName;
        this.conf = conf;
        this.tableName = tableName;
        this.mapper = mapper;
        try {
            this.uniqueKeyFormatter = conf.getUniqueKeyFormatterClass().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Problem instantiating the UniqueKeyFormatter.", e);
        }
        ConfigureUtil.configure(uniqueKeyFormatter, conf.getGlobalParams());
        this.sharder = sharder;
        this.solrWriter = solrWriter;
        this.indexingTimer = Metrics.newTimer(metricName(getClass(),
                "Index update calculation timer", indexerName),
                TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    }

    /**
     * Returns the name of this indexer.
     *
     * @return indexer name
     */
    public String getName() {
        return indexerName;
    }


    /**
     * Build all new documents and ids to delete based on a list of {@code RowData}s.
     *
     * @param rowDataList     list of RowData instances to be considered for indexing
     * @param updateCollector collects updates to be written to Solr
     */
    abstract void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException;


    /**
     * Create index documents based on a nested list of RowData instances.
     *
     * @param rowDataList list of RowData instances to be considered for indexing
     */
    public void indexRowData(List<RowData> rowDataList) throws IOException, SolrServerException, SharderException {
        SolrUpdateCollector updateCollector = new SolrUpdateCollector(rowDataList.size());
        TimerContext timerContext = indexingTimer.time();
        try {
            calculateIndexUpdates(rowDataList, updateCollector);
        } finally {
            timerContext.stop();
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Indexer %s will send to Solr %s adds and %s deletes", getName(),
                    updateCollector.getDocumentsToAdd().size(), updateCollector.getIdsToDelete().size()));
        }

        if (sharder == null) {
            // don't shard
            if (!updateCollector.getDocumentsToAdd().isEmpty()) {
                solrWriter.add(-1, updateCollector.getDocumentsToAdd());
            }
            if (!updateCollector.getIdsToDelete().isEmpty()) {
                solrWriter.deleteById(-1, updateCollector.getIdsToDelete());
            }
        } else {
            // with sharding
            if (!updateCollector.getDocumentsToAdd().isEmpty()) {
                Map<Integer, Map<String, SolrInputDocument>> addsByShard = shardByMapKey(updateCollector.getDocumentsToAdd());
                for (Map.Entry<Integer, Map<String, SolrInputDocument>> entry : addsByShard.entrySet()) {
                    solrWriter.add(entry.getKey(), entry.getValue());
                }
            }
            if (!updateCollector.getIdsToDelete().isEmpty()) {
                Map<Integer, Collection<String>> idsByShard = shardByValue(updateCollector.getIdsToDelete());
                for (Map.Entry<Integer, Collection<String>> entry : idsByShard.entrySet()) {
                    solrWriter.deleteById(entry.getKey(), Lists.newArrayList(entry.getValue()));
                }
            }
        }

        for (String deleteQuery : updateCollector.getDeleteQueries()) {
            solrWriter.deleteByQuery(deleteQuery);
        }

    }

    /**
     * groups a map of (id->document) pairs by shard
     * (consider moving this to a BaseSharder class)
     */
    private Map<Integer, Map<String, SolrInputDocument>> shardByMapKey(Map<String, SolrInputDocument> documentsToAdd)
            throws SharderException {
        Table<Integer, String, SolrInputDocument> table = HashBasedTable.create();

        for (Map.Entry<String, SolrInputDocument> entry : documentsToAdd.entrySet()) {
            table.put(sharder.getShard(entry.getKey()), entry.getKey(), entry.getValue());
        }

        return table.rowMap();
    }

    /**
     * groups a list of ids by shard
     * (consider moving this to a BaseSharder class)
     */
    private Map<Integer, Collection<String>> shardByValue(List<String> idsToDelete) {
        Multimap<Integer, String> map = Multimaps.index(idsToDelete, new Function<String, Integer>() {
            @Override
            public Integer apply(@Nullable String id) {
                try {
                    return sharder.getShard(id);
                } catch (SharderException e) {
                    throw new RuntimeException("error calculating hash", e);
                }
            }
        });
        return map.asMap();
    }

    public void stop() {
        Closer.close(mapper);
        Closer.close(uniqueKeyFormatter);
        IndexerMetricsUtil.shutdownMetrics(indexerName);
    }

    static class RowBasedIndexer extends Indexer {

        private HTablePool tablePool;
        private Timer rowReadTimer;

        public RowBasedIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper,
                               HTablePool tablePool,
                               Sharder sharder, SolrInputDocumentWriter solrWriter) {
            super(indexerName, conf, tableName, mapper, sharder, solrWriter);
            this.tablePool = tablePool;
            rowReadTimer = Metrics.newTimer(metricName(getClass(), "Row read timer", indexerName), TimeUnit.MILLISECONDS,
                    TimeUnit.SECONDS);
        }

        private Result readRow(RowData rowData) throws IOException {
            TimerContext timerContext = rowReadTimer.time();
            try {
                HTableInterface table = tablePool.getTable(rowData.getTable());
                try {
                    Get get = mapper.getGet(rowData.getRow());
                    return table.get(get);
                } finally {
                    table.close();
                }
            } finally {
                timerContext.stop();
            }
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException {

            Map<String, RowData> idToRowData = calculateUniqueEvents(rowDataList);

            for (RowData rowData : idToRowData.values()) {
                String tableName = new String(rowData.getTable(), Charsets.UTF_8);

                Result result = rowData.toResult();
                if (conf.getRowReadMode() == RowReadMode.DYNAMIC) {
                    if (!mapper.containsRequiredData(result)) {
                        result = readRow(rowData);
                    }
                }

                boolean rowDeleted = result.isEmpty();

                String documentId;
                if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                    documentId = ((UniqueTableKeyFormatter) uniqueKeyFormatter).formatRow(rowData.getRow(),
                            rowData.getTable());
                } else {
                    documentId = uniqueKeyFormatter.formatRow(rowData.getRow());
                }

                if (rowDeleted) {
                    // Delete row from Solr as well
                    updateCollector.deleteById(documentId);
                    if (log.isDebugEnabled()) {
                        log.debug("Row " + Bytes.toString(rowData.getRow()) + ": deleted from Solr");
                    }
                } else {
                    IdAddingSolrUpdateWriter idAddingUpdateWriter = new IdAddingSolrUpdateWriter(
                            conf.getUniqueKeyField(),
                            documentId,
                            conf.getTableNameField(),
                            tableName,
                            updateCollector);
                    mapper.map(result, idAddingUpdateWriter);
                }
            }
        }

        /**
         * Calculate a map of Solr document ids to relevant RowData, only taking the most recent event for each document id..
         */
        private Map<String, RowData> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, RowData> idToEvent = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                // Check if the event contains changes to relevant key values
                boolean relevant = false;
                for (KeyValue kv : rowData.getKeyValues()) {
                    if (mapper.isRelevantKV(kv) || kv.isDelete()) {
                        relevant = true;
                        break;
                    }
                }

                if (!relevant) {
                    continue;
                }
                if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                    idToEvent.put(((UniqueTableKeyFormatter) uniqueKeyFormatter).formatRow(rowData.getRow(),
                            rowData.getTable()), rowData);
                } else {
                    idToEvent.put(uniqueKeyFormatter.formatRow(rowData.getRow()), rowData);
                }

            }
            return idToEvent;
        }

    }

    static class ColumnBasedIndexer extends Indexer {

        public ColumnBasedIndexer(String indexerName, IndexerConf conf, String tableName, ResultToSolrMapper mapper,
                                  Sharder sharder, SolrInputDocumentWriter solrWriter) {
            super(indexerName, conf, tableName, mapper, sharder, solrWriter);
        }

        @Override
        protected void calculateIndexUpdates(List<RowData> rowDataList, SolrUpdateCollector updateCollector) throws IOException {
            Map<String, KeyValue> idToKeyValue = calculateUniqueEvents(rowDataList);
            for (Entry<String, KeyValue> idToKvEntry : idToKeyValue.entrySet()) {
                String documentId = idToKvEntry.getKey();

                KeyValue keyValue = idToKvEntry.getValue();
                if (keyValue.isDelete()) {
                    handleDelete(documentId, keyValue, updateCollector, uniqueKeyFormatter);
                } else {
                    Result result = Result.create(Collections.<Cell>singletonList(keyValue));
                    SolrUpdateWriter updateWriter = new RowAndFamilyAddingSolrUpdateWriter(
                            conf.getRowField(),
                            conf.getColumnFamilyField(),
                            uniqueKeyFormatter,
                            keyValue,
                            new IdAddingSolrUpdateWriter(
                                    conf.getUniqueKeyField(),
                                    documentId,
                                    conf.getTableNameField(),
                                    tableName,
                                    updateCollector));

                    mapper.map(result, updateWriter);

                }
            }
        }

        private void handleDelete(String documentId, KeyValue deleteKeyValue, SolrUpdateCollector updateCollector,
                                  UniqueKeyFormatter uniqueKeyFormatter) {
            byte deleteType = deleteKeyValue.getType();
            if (deleteType == KeyValue.Type.DeleteColumn.getCode()) {
                updateCollector.deleteById(documentId);
            } else if (deleteType == KeyValue.Type.DeleteFamily.getCode()) {
                if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                    deleteFamily(deleteKeyValue, updateCollector, uniqueKeyFormatter,
                            ((UniqueTableKeyFormatter) uniqueKeyFormatter).unformatTable(documentId));
                } else {
                    deleteFamily(deleteKeyValue, updateCollector, uniqueKeyFormatter, null);
                }
            } else if (deleteType == KeyValue.Type.Delete.getCode()) {
                if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                    deleteRow(deleteKeyValue, updateCollector, uniqueKeyFormatter,
                            ((UniqueTableKeyFormatter) uniqueKeyFormatter).unformatTable(documentId));
                } else {
                    deleteRow(deleteKeyValue, updateCollector, uniqueKeyFormatter, null);
                }

            } else {
                log.error(String.format("Unknown delete type %d for document %s, not doing anything", deleteType, documentId));
            }
        }

        /**
         * Delete all values for a single column family from Solr.
         */
        private void deleteFamily(KeyValue deleteKeyValue, SolrUpdateCollector updateCollector,
                                  UniqueKeyFormatter uniqueKeyFormatter, byte[] tableName) {
            String rowField = conf.getRowField();
            String cfField = conf.getColumnFamilyField();
            String rowValue;
            String familyValue;
            if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                UniqueTableKeyFormatter uniqueTableKeyFormatter = (UniqueTableKeyFormatter) uniqueKeyFormatter;
                rowValue = uniqueTableKeyFormatter.formatRow(deleteKeyValue.getRow(), tableName);
                familyValue = uniqueTableKeyFormatter.formatFamily(deleteKeyValue.getFamily(), tableName);
            } else {
                rowValue = uniqueKeyFormatter.formatRow(deleteKeyValue.getRow());
                familyValue = uniqueKeyFormatter.formatFamily(deleteKeyValue.getFamily());
            }

            if (rowField != null && cfField != null) {
                updateCollector.deleteByQuery(String.format("(%s:%s)AND(%s:%s)", rowField, rowValue, cfField, familyValue));
            } else {
                log.warn(String.format(
                        "Can't delete row %s and family %s from Solr because row and/or family fields not included in the indexer configuration",
                        rowValue, familyValue));
            }
        }

        /**
         * Delete all values for a single row from Solr.
         */
        private void deleteRow(KeyValue deleteKeyValue, SolrUpdateCollector updateCollector,
                               UniqueKeyFormatter uniqueKeyFormatter, byte[] tableName) {
            String rowField = conf.getRowField();
            String rowValue = uniqueKeyFormatter.formatRow(deleteKeyValue.getRow());
            if (rowField != null) {
                updateCollector.deleteByQuery(String.format("%s:%s", rowField, rowValue));
            } else {
                log.warn(String.format(
                        "Can't delete row %s from Solr because row field not included in indexer configuration",
                        rowValue));
            }
        }

        /**
         * Calculate a map of Solr document ids to KeyValue, only taking the most recent event for each document id.
         */
        private Map<String, KeyValue> calculateUniqueEvents(List<RowData> rowDataList) {
            Map<String, KeyValue> idToKeyValue = Maps.newHashMap();
            for (RowData rowData : rowDataList) {
                for (KeyValue kv : rowData.getKeyValues()) {
                    if (mapper.isRelevantKV(kv)) {
                        String id;
                        if (uniqueKeyFormatter instanceof UniqueTableKeyFormatter) {
                            id = ((UniqueTableKeyFormatter) uniqueKeyFormatter).formatKeyValue(kv, rowData.getTable());
                        } else {
                            id = uniqueKeyFormatter.formatKeyValue(kv);
                        }

                        idToKeyValue.put(id, kv);
                    }
                }
            }
            return idToKeyValue;
        }
    }
}
