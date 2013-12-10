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

import java.util.Map;

import com.ngdata.hbaseindexer.util.IndexerNameValidator;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Defines an indexer within the {@link IndexerModel}.
 *
 * <p>This object is immutable, use {@link IndexerDefinitionBuilder} to construct instances.</p>
 *
 * <p>Operations for loading & storing IndexerDefinition's can be found on {@link IndexerModel}.</p>
 *
 * <p>An important aspect of an indexer definition are the state flags:</p>
 *
 * <ul>
 * <li>{@link LifecycleState}</li>
 * <li>{@link BatchIndexingState}</li>
 * <li>{@link IncrementalIndexingState}</li>
 * </ul>
 *
 * <p>Some operations, like triggering a batch index rebuild or deleting an index or done
 * by changing these state flags, rather than by direct method/rpc calls.</p>
 */
public class IndexerDefinition {
    private String name;
    private LifecycleState lifecycleState = LifecycleState.ACTIVE;
    private BatchIndexingState batchIndexingState = BatchIndexingState.INACTIVE;
    private IncrementalIndexingState incrementalIndexingState = IncrementalIndexingState.SUBSCRIBE_AND_CONSUME;
    private String subscriptionId;
    private String indexerComponentFactory;
    private byte[] configuration;
    private String connectionType;
    private Map<String, String> connectionParams;
    private String[] defaultBatchIndexCliArguments;
    private String[] batchIndexCliArguments;
    private BatchBuildInfo lastBatchBuildInfo;
    private BatchBuildInfo activeBatchBuildInfo;
    private long subscriptionTimestamp;
    private int occVersion = 0; // not -1 by default, since otherwise users might unintentionally disable OCC

    /**
     * Use {@link IndexerDefinitionBuilder} to make instances of this class.
     */
    IndexerDefinition(String name,
                      LifecycleState lifecycleState,
                      BatchIndexingState batchIndexingState,
                      IncrementalIndexingState incrementalIndexingState,
                      String subscriptionId,
                      byte[] configuration,
                      String indexerComponentFactory,
                      String connectionType,
                      Map<String, String> connectionParams,
                      String[] defaultBatchIndexCliArguments,
                      String[] batchIndexCliArguments,
                      BatchBuildInfo lastBatchBuildInfo,
                      BatchBuildInfo activeBatchBuildInfo,
                      long subscriptionTimestamp,
                      int occVersion) {
        IndexerNameValidator.validate(name);
        this.name = name;
        this.lifecycleState = lifecycleState;
        this.batchIndexingState = batchIndexingState;
        this.incrementalIndexingState = incrementalIndexingState;
        this.subscriptionId = subscriptionId;
        this.configuration = configuration;
        this.indexerComponentFactory = indexerComponentFactory;
        this.connectionType = connectionType;
        this.connectionParams = connectionParams;
        this.defaultBatchIndexCliArguments = defaultBatchIndexCliArguments;
        this.batchIndexCliArguments = batchIndexCliArguments;
        this.lastBatchBuildInfo = lastBatchBuildInfo;
        this.activeBatchBuildInfo = activeBatchBuildInfo;
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.occVersion = occVersion;
    }

    public String getName() {
        return name;
    }

    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    public BatchIndexingState getBatchIndexingState() {
        return batchIndexingState;
    }

    public IncrementalIndexingState getIncrementalIndexingState() {
        return incrementalIndexingState;
    }

    /**
     * The ID of a subscription on an event stream.
     *
     * <p>Typically this will be a subscription on the HBase SEP (Side Effect Processor), but it could be
     * another system as well, for the IndexerDefinition this is just a string.</p>
     */
    public String getSubscriptionId() {
        return subscriptionId;
    }

    /**
     * The configuration for the indexer process, typically defines how to indexing algorithm should
     * behave and how it should map input records to the index. From the point of view of the IndexerDefinition,
     * this is an opaque byte array.
     *
     * <p>Callers should not modify the returned byte array.</p>
     */
    public byte[] getConfiguration() {
        return configuration;
    }

    /**
     * Identifies the type of indexing engine this indexer connects to, e.g. "solr".
     */
    public String getConnectionType() {
        return connectionType;
    }

    /**
     * A free-form set of connection parameters, contents depends on what is supported by the
     * {@link #getConnectionType()}. For example, in case of SOLR this could contain the URL's of
     * the Solr instances, or in case of SolrCloud it could just a zookeeper connection string
     * and a collection name.
     */
    public Map<String, String> getConnectionParams() {
        return connectionParams;
    }

    /**
     * Get the version number used for optimistic concurrency control (OCC).
     *
     * <p>Practically speaking, in the current implementation this is the version of the ZooKeeper node
     * in which the data is stored. When using a read-alter-write cycle, this will unsure nobody else
     * modified the IndexerDefinition concurrently.</p>
     *
     * <p>Setting the version to -1 will disable the OCC check.</p>
     */
    public int getOccVersion() {
        return occVersion;
    }

    /**
     * Status information on the last run batch indexing build. This can be null if no batch index build ever ran.
     *
     * <p>This information is written by the system, not by clients.</p>
     */
    public BatchBuildInfo getLastBatchBuildInfo() {
        return lastBatchBuildInfo;
    }

    /**
     * Status information on the currently running batch indexing build, if any, otherwise this returns null.
     *
     * <p>This information is written by the system, not by clients.</p>
     */
    public BatchBuildInfo getActiveBatchBuildInfo() {
        return activeBatchBuildInfo;
    }

    /**
     * CLI arguments for the next batch index build.
     *
     * <p>These parameters are used once. Typically you will set these parameters together with updating
     * the {@link #getBatchIndexingState() batch indexing state} to {@link BatchIndexingState#BUILD_REQUESTED}.
     * After the system started the batch index build, it will reset this property to null.</p>
     *
     * <p>There are also {@link #getDefaultBatchIndexCliArguments() default parameters} that are used in case
     * this is not set.</p>
     *
     * @see HBaseMapReduceIndexerTool
     */
    public String[] getBatchIndexCliArguments() {
        return batchIndexCliArguments;
    }

    /**
     * Default CLI arguments for batch index builds.
     *
     * <p>These parameters can be overridden for individual batch indexing runs by the ones in
     * {@link #getBatchIndexCliArguments()}.</p>
     */
    public String[] getDefaultBatchIndexCliArguments() {
        return defaultBatchIndexCliArguments;
    }

    public String[] getBatchIndexCliArgumentsOrDefault() {
        if (batchIndexCliArguments != null)
            return batchIndexCliArguments;
        else if (defaultBatchIndexCliArguments != null)
            return defaultBatchIndexCliArguments;
        else
            return new String[0];
    }

    /**
     * The timestamp of when this indexer's subscription started. Only record updates that have
     * occurred after this timestamp will be consumed by this index.
     *
     * <p>This is useful for certain subscription implementations, such as the HBase SEP, which can possibly
     * play back older events (in the case of the HBase SEP, events from the start of the current HLog are
     * delivered).</p>
     *
     * @return Number of milliseconds since the epoch
     */
    public long getSubscriptionTimestamp() {
        return subscriptionTimestamp;
    }

    public String getIndexerComponentFactory() {
        return indexerComponentFactory;
    }

    @Override
    public boolean equals(Object obj) {
        return EqualsBuilder.reflectionEquals(this, obj);
    }

    @Override
    public int hashCode() {
        return HashCodeBuilder.reflectionHashCode(this);
    }

    @Override
    public String toString() {
        return "Indexer '" + name + "', state=" + lifecycleState;
    }

    public static enum LifecycleState {
        /**
         * The indexer exists. This is the default lifecycle state.
         */
        ACTIVE,

        /**
         * A client puts the LifecyleState to DELETE_REQUESTED to request deletion of an indexer.
         */
        DELETE_REQUESTED,

        /**
         * Indicates the delete request is being processed.
         *
         * <p>This state is set by the server, not by clients.</p>
         */
        DELETING,

        /**
         * Indicates a delete request failed, in which case more information can be found in the logs,
         * set again to {@link #DELETE_REQUESTED} to retry.
         *
         * <p>This state is set by the server, not by clients.</p>
         */
        DELETE_FAILED;

        public static final LifecycleState DEFAULT = ACTIVE;

        public boolean isDeleteState() {
            return this == LifecycleState.DELETE_REQUESTED
                    || this == LifecycleState.DELETING
                    || this == LifecycleState.DELETE_FAILED;
        }
    }

    public static enum IncrementalIndexingState {
        /**
         * An event subscription is taken, and the events are consumed, in other words, the incremental
         * indexing is enabled. This is the most common state.
         */
        SUBSCRIBE_AND_CONSUME,

        /**
         * An event subscription is taken, but the events are not consumed. The events will queue up until
         * the state is moved to {@link #SUBSCRIBE_AND_CONSUME}. This state can be used when you want to
         * temporarily disable indexing without loosing any events. Typically should only be used during
         * short durations (depends on the characteristics of the underlying event queueing mechanism).
         */
        SUBSCRIBE_DO_NOT_CONSUME,

        /**
         * No event subscription is taken, incremental indexing is disabled.
         */
        DO_NOT_SUBSCRIBE;

        public static final IncrementalIndexingState DEFAULT = SUBSCRIBE_AND_CONSUME;
    }

    public static enum BatchIndexingState {
        /**
         * No batch indexing job is running or requested to run.
         */
        INACTIVE,

        /**
         * A client puts the BatchIndexingState to BUILD_REQUESTED to request the startup of a batch indexing
         * job.
         */
        BUILD_REQUESTED,

        /**
         * Indicates a batch indexing job is running. More information on the running job can be found in
         * {@link IndexerDefinition#getActiveBatchBuildInfo()}.
         *
         * <p>This state is set by the server, not by clients.</p>
         */
        BUILDING;

        public static final BatchIndexingState DEFAULT = INACTIVE;
    }
}
