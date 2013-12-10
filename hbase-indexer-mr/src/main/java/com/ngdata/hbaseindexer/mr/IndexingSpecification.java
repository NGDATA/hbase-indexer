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
package com.ngdata.hbaseindexer.mr;

import java.util.Map;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

import com.google.common.base.Preconditions;

/**
 * Encapsulates the definition of the HBase indexing to be done.
 * <p>
 * A separate class than the ZooKeeper-based indexing configuration is used because
 * the parameter contained in this class may come from a combination of ZooKeeper
 * and/or command-line arguments.
 */
class IndexingSpecification {
    
    private final String tableName;
    
    private final String indexerName;

    private final String indexerComponentFactory;

    private final byte[] configuration;
    
    private final Map<String,String> indexConnectionParams;
    
    /**
     * @param tableName name of the HBase table to be indexed
     * @param indexerName name of the indexer being executed
     * @param indexerComponentFactory
     * @param indexConnectionParams free-form index connection parameters
     */
    public IndexingSpecification(String tableName, String indexerName, String indexerComponentFactory, byte[] configuration, Map<String, String> indexConnectionParams) {
        Preconditions.checkNotNull(tableName);
        Preconditions.checkNotNull(indexerName);
        Preconditions.checkNotNull(configuration);
        Preconditions.checkNotNull(indexConnectionParams);
        this.tableName = tableName;
        this.indexerName = indexerName;
        this.indexerComponentFactory = indexerComponentFactory;
        this.configuration = configuration;
        this.indexConnectionParams = indexConnectionParams;
    }
    
    
    /**
     * Get the name of the table to be indexed.
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Get the name of the indexer to be executed.
     */
    public String getIndexerName() {
        return indexerName;
    }

    String getIndexerComponentFactory() {
        return indexerComponentFactory;
    }

    /**
     * Get the contents of the indexer's xml definition.
     */
    public byte[] getConfiguration() {
        return configuration;
    }

    /**
     * Get the free-from index connection parameters.
     */
    public Map<String, String> getIndexConnectionParams() {
        return indexConnectionParams;
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
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
