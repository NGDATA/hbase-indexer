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
package com.ngdata.hbaseindexer.conf;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;

import com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;
import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.uniquekey.StringUniqueKeyFormatter;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;

/**
 * The configuration for an indexer, i.e. this defines the behavior of the {@link Indexer} and of the parser/mapper
 * called by the indexer.
 *
 * <p>Instances of IndexerConf can be created using {@link IndexerConfBuilder} or from XML using an
 * {@link IndexerComponentFactory}.</p>
 */
public class IndexerConf {
    private String table;
    private MappingType mappingType;
    private RowReadMode rowReadMode;
    private String uniqueKeyField;
    private String rowField;
    private String columnFamilyField;
    private String tableNameField;
    private Class<? extends ResultToSolrMapper> mapperClass;
    private Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass;
    private List<FieldDefinition> fieldDefinitions;
    private List<DocumentExtractDefinition> extractDefinitions;
    private Map<String, String> globalParams;

    public enum MappingType { ROW, COLUMN }

    /**
     * Mode for deciding if a row should be re-read after a mutation event occurs on it. This mode setting is only
     * applicable for row-based indexing.
     */
    public enum RowReadMode {

        /**
         * Re-read a row to be indexed after a mutation event has occurred on the row if the mutation event itself does
         * not include sufficient information to perform indexing.
         */
        DYNAMIC,

        /**
         * Never re-read a row to be indexed after a mutation event.
         */
        NEVER
    }

    public static final MappingType DEFAULT_MAPPING_TYPE = MappingType.ROW;
    public static final RowReadMode DEFAULT_ROW_READ_MODE = RowReadMode.DYNAMIC;
    public static final String DEFAULT_UNIQUE_KEY_FIELD = "id";
    public static final Class<? extends UniqueKeyFormatter> DEFAULT_UNIQUE_KEY_FORMATTER = StringUniqueKeyFormatter.class;
    public static final ValueSource DEFAULT_VALUE_SOURCE = ValueSource.VALUE;
    public static final String DEFAULT_FIELD_TYPE = "string";
    public static final String DEFAULT_EXTRACT_TYPE = "application/octet-stream";

    IndexerConf(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public MappingType getMappingType() {
        return mappingType;
    }

    public RowReadMode getRowReadMode() {
        return rowReadMode;
    }

    public String getUniqueKeyField() {
        return uniqueKeyField;
    }
    
    /**
     * Get the name of the Solr field used to identify the HBase row containing the
     * indexed document. Only used for column-based indexing.
     */
    public String getRowField() {
        return rowField;
    }

    /**
     * Get the name of the Solr field used to identify the HBase column family containing
     * the indexed document. Only used for column-based indexing.
     */
    public String getColumnFamilyField() {
        return columnFamilyField;
    }
    
    /**
     * Get the name of the Solr field used to identify the HBase table containing the
     * indexed document.
     */
    public String getTableNameField() {
        return tableNameField;
    }
    
    public Class<? extends ResultToSolrMapper> getMapperClass() {
        return mapperClass;
    }

    public Class<? extends UniqueKeyFormatter> getUniqueKeyFormatterClass() {
        return uniqueKeyFormatterClass;
    }

    public List<FieldDefinition> getFieldDefinitions() {
        return fieldDefinitions;
    }
    
    public List<DocumentExtractDefinition> getDocumentExtractDefinitions() {
        return extractDefinitions;
    }
    
    /**
     * Get the map of global configuration parameters for this indexer.
     */
    public Map<String, String> getGlobalParams() {
        return globalParams;
    }

    void setMappingType(MappingType mappingType) {
        this.mappingType = mappingType;
    }

    void setRowReadMode(RowReadMode rowReadMode) {
        this.rowReadMode = rowReadMode;
    }
    
     void setMapperClass(Class<? extends ResultToSolrMapper> mapperClass) {
        this.mapperClass = mapperClass;
    }

    void setUniqueKeyField(String uniqueKeyField) {
        this.uniqueKeyField = uniqueKeyField;
    }
    
    void setRowField(String rowField) {
        this.rowField = rowField;
    }
    
    public void setColumnFamilyField(String columnFamilyField) {
        this.columnFamilyField = columnFamilyField;
    }
    
    public void setTableNameField(String tableNameField) {
        this.tableNameField = tableNameField;
    }
    
    void setFieldDefinitions(List<FieldDefinition> fieldDefinitions) {
        this.fieldDefinitions = Collections.unmodifiableList(fieldDefinitions);
    }
    
    void setDocumentExtractDefinitions(List<DocumentExtractDefinition> extractDefinitions) {
        this.extractDefinitions = Collections.unmodifiableList(extractDefinitions);
    }

    void setUniqueKeyFormatterClass(Class<? extends UniqueKeyFormatter> uniqueKeyFormatterClass) {
        this.uniqueKeyFormatterClass = uniqueKeyFormatterClass;
    }
    
    public void setGlobalParams(Map<String, String> globalParams) {
        this.globalParams = globalParams;
    }

   
}
