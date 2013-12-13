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
package com.ngdata.hbaseindexer.impl;

import static junit.framework.Assert.assertNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.common.collect.ImmutableMap;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionJsonSerDeser;
import org.junit.Test;

public class IndexerDefinitionJsonSerDeserTest {
    @Test
    public void testMinimal() {
        IndexerDefinition indexer = new IndexerDefinitionBuilder()
                .name("index1").build();

        IndexerDefinitionJsonSerDeser serdeser = new IndexerDefinitionJsonSerDeser();
        byte[] json = serdeser.toJsonBytes(indexer);

        IndexerDefinition indexer2 = serdeser.fromJsonBytes(json).build();

        assertEquals(indexer, indexer2);
        assertEquals("index1", indexer.getName());
    }

    @Test
    public void testFull() {
        IndexerDefinition indexer = new IndexerDefinitionBuilder()
                .name("index1")
                .lifecycleState(LifecycleState.DELETE_REQUESTED)
                .batchIndexingState(BatchIndexingState.BUILDING)
                .incrementalIndexingState(IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME)
                .indexerComponentFactory("testReader")
                .configuration("config1".getBytes())
                .connectionType("solr")
                .connectionParams(ImmutableMap.of("p1", "v1", "p2", "v2"))
                .subscriptionId("my-subscription")
                .subscriptionTimestamp(5L)
                .defaultBatchIndexCliArguments(new String[]{"arg1", "arg2"})
                .batchIndexCliArguments(new String[]{"arg3"})
                .activeBatchBuildInfo(
                        new BatchBuildInfo(10L, null, ImmutableMap.of("job-id-1", "url-1"), new String[]{"arg1", "arg2"}))
                .lastBatchBuildInfo(
                        new BatchBuildInfo(11L, false, ImmutableMap.of("job-id-2", "url-2"), new String[]{"arg3"}))
                .occVersion(5).build();

        IndexerDefinitionJsonSerDeser serdeser = new IndexerDefinitionJsonSerDeser();
        byte[] json = serdeser.toJsonBytes(indexer);

        IndexerDefinition indexer2 = serdeser.fromJsonBytes(json).build();

        assertEquals("index1", indexer2.getName());
        assertEquals(LifecycleState.DELETE_REQUESTED, indexer2.getLifecycleState());
        assertEquals(BatchIndexingState.BUILDING, indexer2.getBatchIndexingState());
        assertEquals(IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME, indexer2.getIncrementalIndexingState());
        assertEquals("testReader", indexer2.getIndexerComponentFactory());
        assertArrayEquals("config1".getBytes(), indexer2.getConfiguration());
        assertEquals("solr", indexer.getConnectionType());
        assertEquals("v1", indexer.getConnectionParams().get("p1"));
        assertEquals("v2", indexer.getConnectionParams().get("p2"));
        assertEquals("my-subscription", indexer2.getSubscriptionId());
        assertEquals(5L, indexer2.getSubscriptionTimestamp());
        assertArrayEquals(new String[]{"arg1", "arg2"}, indexer2.getDefaultBatchIndexCliArguments());
        assertArrayEquals(new String[]{"arg3"}, indexer2.getBatchIndexCliArguments());

        assertNotNull(indexer2.getActiveBatchBuildInfo());
        assertEquals(10L, indexer2.getActiveBatchBuildInfo().getSubmitTime());
        assertEquals(ImmutableMap.of("job-id-1", "url-1"), indexer2.getActiveBatchBuildInfo().getMapReduceJobTrackingUrls());
        assertNull(indexer2.getActiveBatchBuildInfo().isFinishedSuccessful());
        assertArrayEquals(new String[]{"arg1", "arg2"}, indexer2.getActiveBatchBuildInfo().getBatchIndexCliArguments());

        assertNotNull(indexer2.getLastBatchBuildInfo());
        assertEquals(11L, indexer2.getLastBatchBuildInfo().getSubmitTime());
        assertEquals(ImmutableMap.of("job-id-2", "url-2"), indexer2.getLastBatchBuildInfo().getMapReduceJobTrackingUrls());
        assertFalse(indexer2.getLastBatchBuildInfo().isFinishedSuccessful());
        assertArrayEquals(new String[]{"arg3"}, indexer2.getLastBatchBuildInfo().getBatchIndexCliArguments());

        assertEquals(5, indexer2.getOccVersion());

        assertEquals(indexer, indexer2);
    }
}
