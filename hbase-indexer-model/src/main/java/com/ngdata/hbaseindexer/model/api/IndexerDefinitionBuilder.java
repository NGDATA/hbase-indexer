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
package com.ngdata.hbaseindexer.model.api;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

public class IndexerDefinitionBuilder {
    private String name;
    private LifecycleState lifecycleState = LifecycleState.ACTIVE;
    private BatchIndexingState batchIndexingState = BatchIndexingState.INACTIVE;
    private IncrementalIndexingState incrementalIndexingState = IncrementalIndexingState.SUBSCRIBE_AND_CONSUME;
    private String subscriptionId;
    private byte[] configuration;
    private String indexerComponentFactory;
    private String connectionType;
    private Map<String, String> connectionParams;
    private String[] defaultBatchIndexCliArguments;
    private String[] batchIndexCliArguments;
    private BatchBuildInfo lastBatchBuildInfo;
    private BatchBuildInfo activeBatchBuildInfo;
    private long subscriptionTimestamp;
    private int occVersion = -1;

    public IndexerDefinitionBuilder startFrom(IndexerDefinition existingDefinition) {
        this.name = existingDefinition.getName();
        this.lifecycleState = existingDefinition.getLifecycleState();
        this.batchIndexingState = existingDefinition.getBatchIndexingState();
        this.incrementalIndexingState = existingDefinition.getIncrementalIndexingState();
        this.subscriptionId = existingDefinition.getSubscriptionId();
        this.configuration = existingDefinition.getConfiguration();
        this.indexerComponentFactory = existingDefinition.getIndexerComponentFactory();
        this.connectionType = existingDefinition.getConnectionType();
        this.connectionParams = existingDefinition.getConnectionParams();
        this.defaultBatchIndexCliArguments = existingDefinition.getDefaultBatchIndexCliArguments();
        this.batchIndexCliArguments = existingDefinition.getBatchIndexCliArguments();
        this.lastBatchBuildInfo = existingDefinition.getLastBatchBuildInfo();
        this.activeBatchBuildInfo = existingDefinition.getActiveBatchBuildInfo();
        this.subscriptionTimestamp = existingDefinition.getSubscriptionTimestamp();
        this.occVersion = existingDefinition.getOccVersion();
        return this;
    }

    /**
     * @see IndexerDefinition#getName()
     */
    public IndexerDefinitionBuilder name(String name) {
        this.name = name;
        return this;
    }

    /**
     * @see IndexerDefinition#getLifecycleState()
     */
    public IndexerDefinitionBuilder lifecycleState(LifecycleState state) {
        this.lifecycleState = state;
        return this;
    }

    /**
     * @see IndexerDefinition#getIncrementalIndexingState()
     */
    public IndexerDefinitionBuilder incrementalIndexingState(IncrementalIndexingState state) {
        this.incrementalIndexingState = state;
        return this;
    }

    /**
     * @see IndexerDefinition#getBatchIndexingState()
     */
    public IndexerDefinitionBuilder batchIndexingState(BatchIndexingState state) {
        this.batchIndexingState = state;
        return this;
    }

    /**
     * @see IndexerDefinition#subscriptionId
     */
    public IndexerDefinitionBuilder subscriptionId(String subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }

    /**
     * @see IndexerDefinition#getConfiguration()
     */
    public IndexerDefinitionBuilder configuration(byte[] configuration) {
        this.configuration = configuration;
        return this;
    }

    /**
     * @see IndexerDefinition#getConnectionType()
     */
    public IndexerDefinitionBuilder connectionType(String connectionType) {
        this.connectionType = connectionType;
        return this;
    }

    /**
     * @see IndexerDefinition#getConnectionParams()
     */
    public IndexerDefinitionBuilder connectionParams(Map<String, String> connectionParams) {
        this.connectionParams = connectionParams != null ? ImmutableMap.copyOf(connectionParams) : null;
        return this;
    }

    /**
     * @see IndexerDefinition#lastBatchBuildInfo
     */
    public IndexerDefinitionBuilder lastBatchBuildInfo(BatchBuildInfo info) {
        this.lastBatchBuildInfo = info;
        return this;
    }

    /**
     * @see IndexerDefinition#activeBatchBuildInfo
     */
    public IndexerDefinitionBuilder activeBatchBuildInfo(BatchBuildInfo info) {
        this.activeBatchBuildInfo = info;
        return this;
    }

    /**
     * @see IndexerDefinition#defaultBatchIndexCliArguments
     */
    public IndexerDefinitionBuilder defaultBatchIndexCliArguments(String[] defaultBatchIndexCliArguments) {
        this.defaultBatchIndexCliArguments = defaultBatchIndexCliArguments;
        return this;
    }

    /**
     * @see IndexerDefinition#batchIndexCliArguments
     */
    public IndexerDefinitionBuilder batchIndexCliArguments(String[] batchIndexCliArguments) {
        this.batchIndexCliArguments = batchIndexCliArguments;
        return this;
    }

    /**
     * @see IndexerDefinition#subscriptionTimestamp
     */
    public IndexerDefinitionBuilder subscriptionTimestamp(long timestamp) {
        this.subscriptionTimestamp = timestamp;
        return this;
    }

    /**
     * @see IndexerDefinition#indexerComponentFactory
     */
    public IndexerDefinitionBuilder indexerComponentFactory(String indexerComponentFactory) {
        this.indexerComponentFactory = indexerComponentFactory;
        return this;
    }

    /**
     * @see IndexerDefinition#occVersion
     */
    public IndexerDefinitionBuilder occVersion(int occVersion) {
        this.occVersion = occVersion;
        return this;
    }

    public IndexerDefinition build() {
        Preconditions.checkNotNull(name, "name");
        Preconditions.checkNotNull(lifecycleState, "lifecycleState");
        Preconditions.checkNotNull(batchIndexingState, "batchIndexingState");
        Preconditions.checkNotNull(incrementalIndexingState, "incrementalIndexingState");

        return new IndexerDefinition(name, lifecycleState, batchIndexingState, incrementalIndexingState, subscriptionId,
                configuration, indexerComponentFactory, connectionType, connectionParams, defaultBatchIndexCliArguments,
                batchIndexCliArguments, lastBatchBuildInfo, activeBatchBuildInfo, subscriptionTimestamp, occVersion);
    }
}
