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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.IndexerConf.RowReadMode;
import com.ngdata.hbaseindexer.conf.IndexerConfBuilder;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class IndexingEventListenerTest {

    private static final String TABLE_A = "_table_a_";
    private static final String TABLE_B = "_table_b_";

    private SolrInputDocumentWriter solrDocumentWriter;
    private HTablePool tablePool;
    private HTableInterface tableA;
    private HTableInterface tableB;

    @Before
    public void setUp() {
        solrDocumentWriter = mock(SolrInputDocumentWriter.class);
        tablePool = mock(HTablePool.class);
        tableA = mock(HTableInterface.class);
        tableB = mock(HTableInterface.class);
        when(tablePool.getTable(TABLE_A.getBytes(Charsets.UTF_8))).thenReturn(tableA);
        when(tablePool.getTable(TABLE_B.getBytes(Charsets.UTF_8))).thenReturn(tableB);
    }

    /**
     * When receiving an event for a different table than the one specified in the IndexerConf, the Indexer should not
     * interact with HBase or Solr.
     */
    @Test
    public void testNonmatchedTable() {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).build();

        Indexer indexer = Indexer.createIndexer("index name", conf, TABLE_A, null, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_B), null, null, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        verifyZeroInteractions(tableA, tableB, solrDocumentWriter);
    }

    /**
     * When we get a sep event for a row which does not exist anymore (or at least, doesn't contain any relevant
     * columns), then we expect a delete on Solr.
     */
    @Test
    public void testNonExistingRow() throws Exception {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).build();

        when(tableA.get(any(Get.class))).thenReturn(new Result());

        ResultToSolrMapper mapper = mock(ResultToSolrMapper.class);
        when(mapper.isRelevantKV(any(KeyValue.class))).thenReturn(true);
        Indexer indexer = Indexer.createIndexer("index name", conf, "record", mapper, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        verify(solrDocumentWriter).deleteById(-1, Collections.singletonList("row1"));
        verifyNoMoreInteractions(solrDocumentWriter);
    }

    /**
     * Create a dummy HBaseToSolrMapper that returns the given value on any calls to containsRequiredData().
     */
    static ResultToSolrMapper createHbaseToSolrMapper(final boolean containsRequiredDataReturnVal) {
        return new ResultToSolrMapper() {
            @Override
            public boolean isRelevantKV(KeyValue kv) {
                return true;
            }

            @Override
            public Get getGet(byte[] row) {
                return new Get(row);
            }

            @Override
            public void map(Result result, SolrUpdateWriter solrUpdateWriter) {
                solrUpdateWriter.add(new SolrInputDocument());
            }

            @Override
            public boolean containsRequiredData(Result result) {
                return containsRequiredDataReturnVal;
            }
        };
    }

    @Test
    public void testRowBasedIndexing_RowReadModeNever() throws SolrServerException, IOException {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.NEVER).build();

        ResultToSolrMapper mapper = createHbaseToSolrMapper(true);

        Indexer indexer = Indexer.createIndexer("index name", conf, "record", mapper, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("val")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        ArgumentCaptor<Map> addedDocumentsCaptor = ArgumentCaptor.forClass(Map.class);
        verify(solrDocumentWriter).add(eq(-1), addedDocumentsCaptor.capture());
        Map<String, SolrInputDocument> addedDocuments = addedDocumentsCaptor.getValue();
        assertEquals(1, addedDocuments.size());
        assertEquals("row1", addedDocuments.get("row1").getFieldValue("id"));

        verifyZeroInteractions(tableA, tableB);
    }

    @Test
    public void testRowBasedIndexing_RowReadModeDynamic_RereadRequired() throws IOException, SolrServerException {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.DYNAMIC).build();

        ResultToSolrMapper mapper = createHbaseToSolrMapper(false);

        when(tableA.get(any(Get.class))).thenReturn(Result.create(Lists.<Cell>newArrayList(new KeyValue())));

        Indexer indexer = Indexer.createIndexer("index name", conf, "record", mapper, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        ArgumentCaptor<Map> arg = ArgumentCaptor.forClass(Map.class);
        verify(solrDocumentWriter).add(eq(-1), arg.capture());
        Map<String,SolrInputDocument> addedDocuments = arg.getValue();
        assertEquals(1, addedDocuments.size());
        assertEquals("row1", addedDocuments.get("row1").getFieldValue("id"));

        // Should have been called twice -- once during the setup, and once during the test itself
        verify(tableA).get(any(Get.class));
    }

    @Test
    public void testRowBasedIndexing_RowReadModeDynamic_NoRereadRequired() throws SolrServerException, IOException {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).rowReadMode(RowReadMode.DYNAMIC).build();

        ResultToSolrMapper mapper = createHbaseToSolrMapper(true);

        Indexer indexer = Indexer.createIndexer("index name", conf, "record", mapper, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        List<KeyValue> kvs = Lists.newArrayList(new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("cf"),
                Bytes.toBytes("qual"), Bytes.toBytes("value")));
        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        ArgumentCaptor<Map> arg = ArgumentCaptor.forClass(Map.class);
        verify(solrDocumentWriter).add(eq(-1), arg.capture());
        Map<String,SolrInputDocument> addedDocuments = arg.getValue();
        assertEquals(1, addedDocuments.size());
        assertEquals("row1", addedDocuments.get("row1").getFieldValue("id"));

        verifyZeroInteractions(tableA, tableB);
    }

    @Test
    public void testColumnBasedIndexing() throws Exception {
        IndexerConf conf = new IndexerConfBuilder().table(TABLE_A).mappingType(IndexerConf.MappingType.COLUMN).build();

        ResultToSolrMapper mapper = new ResultToSolrMapper() {
            @Override
            public boolean isRelevantKV(KeyValue kv) {
                return Bytes.toString(kv.getFamily()).equals("messages");
            }

            @Override
            public Get getGet(byte[] row) {
                return new Get(row);
            }

            @Override
            public void map(Result result, SolrUpdateWriter solrUpdateWriter) {
                solrUpdateWriter.add(new SolrInputDocument());
            }

            @Override
            public boolean containsRequiredData(Result result) {
                throw new UnsupportedOperationException("Not implemented");
            }
        };

        Indexer indexer = Indexer.createIndexer("index name", conf, TABLE_A, mapper, null, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, TABLE_A, false);

        List<KeyValue> kvs = Lists.newArrayList(
                new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"), Bytes.toBytes("msg1"),
                        Bytes.toBytes("the message")), new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"),
                        Bytes.toBytes("msg2"), Bytes.toBytes("another message")));

        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_A), Bytes.toBytes("row1"), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));

        ArgumentCaptor<Map> arg = ArgumentCaptor.forClass(Map.class);
        verify(solrDocumentWriter).add(eq(-1), arg.capture());
        Map<String,SolrInputDocument> docs = arg.getValue();
        assertEquals(2, docs.size());
        Set<String> documentIds = Sets.newHashSet(Collections2.transform(docs.values(), new Function<SolrInputDocument,String>(){

            @Override
            public String apply(@Nullable SolrInputDocument input) {
                return (String)input.getFieldValue("id");
            }}));
        assertEquals(Sets.newHashSet("row1-messages-msg1","row1-messages-msg2"), documentIds);
    }

    @Test
    public void testMultitableEvents() throws Exception {
        final String tablePrefix = "_multitable_";
        final String multiTableAName = tablePrefix + "a_";
        final String multiTableBName = tablePrefix + "_multitable_b_";

        final HTableInterface multitableA = mock(HTableInterface.class);
        final HTableInterface multitableB = mock(HTableInterface.class);
        when(tablePool.getTable(multiTableAName.getBytes(Charsets.UTF_8))).thenReturn(multitableA);
        when(tablePool.getTable(multiTableBName.getBytes(Charsets.UTF_8))).thenReturn(multitableB);

        final IndexerConf indexerConf = new IndexerConfBuilder().table(tablePrefix + ".*").build();
        ResultToSolrMapper mapper = createHbaseToSolrMapper(true);
        Indexer indexer = Indexer.createIndexer("index name", indexerConf, tablePrefix+ ".*", mapper, tablePool, null, solrDocumentWriter);
        IndexingEventListener indexingEventListener = new IndexingEventListener(indexer, tablePrefix+ ".*", true);
        List<KeyValue> kvs = Lists.newArrayList(
                new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"), Bytes.toBytes("msg1"),
                        Bytes.toBytes("the message")), new KeyValue(Bytes.toBytes("row1"), Bytes.toBytes("messages"),
                Bytes.toBytes("msg2"), Bytes.toBytes("another message")));

        SepEvent event = new SepEvent(Bytes.toBytes(TABLE_B), null, null, null);
        indexingEventListener.processEvents(Collections.singletonList(event));
        verifyZeroInteractions(multitableA, multitableB, tableB, solrDocumentWriter);

        event = new SepEvent(Bytes.toBytes(multiTableBName), "row1".getBytes(Charsets.UTF_8), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));
        verify(solrDocumentWriter, atLeastOnce()).add(eq(-1), anyMap());

        event = new SepEvent(Bytes.toBytes(multiTableAName), "row1".getBytes(Charsets.UTF_8), kvs, null);
        indexingEventListener.processEvents(Collections.singletonList(event));
        verify(solrDocumentWriter, atLeastOnce()).add(eq(-1), anyMap());
    }
}
