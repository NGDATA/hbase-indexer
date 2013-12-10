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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;
import com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper;
import com.ngdata.hbaseindexer.uniquekey.HexUniqueKeyFormatter;
import org.junit.Test;

public class DefaultIndexerComponentFactoryTest {

    private static final String impl = DefaultIndexerComponentFactory.class.getName();

    @Test
    public void testValid() throws Exception {
        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(impl, asStream("<indexer table='foo'/>"), Maps.<String, String>newHashMap());
        factory.createIndexerConf();
    }

    @Test(expected = IndexerConfException.class)
    public void testInvalid() throws Exception {
        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(impl, asStream("<foo/>"), Maps.<String, String>newHashMap());
        factory.createIndexerConf();
    }

    private InputStream asStream(String data) {
        return new ByteArrayInputStream(data.getBytes());
    }

    @Test
    public void testFullIndexerConf() throws Exception {
        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(impl, getClass().getResourceAsStream("indexerconf_full.xml"), Maps.<String, String>newHashMap());
        IndexerConf conf = factory.createIndexerConf();

        assertEquals("table1", conf.getTable());
        assertEquals(IndexerConf.MappingType.COLUMN, conf.getMappingType());
        assertEquals(IndexerConf.RowReadMode.NEVER, conf.getRowReadMode());
        assertEquals("custom-id", conf.getUniqueKeyField());
        assertEquals("custom-row", conf.getRowField());
        assertEquals("custom-family", conf.getColumnFamilyField());
        assertEquals("custom-table-name", conf.getTableNameField());
        assertEquals(HexUniqueKeyFormatter.class, conf.getUniqueKeyFormatterClass());
        assertEquals(TestResultToSolrMapper.class, conf.getMapperClass());

        List<FieldDefinition> fieldDefs = conf.getFieldDefinitions();
        List<FieldDefinition> expectedFieldDefs = Lists.newArrayList(
                new FieldDefinition("field1", "col:qual1", ValueSource.QUALIFIER, "float"),
                new FieldDefinition("field2", "col:qual2", ValueSource.VALUE, "long",
                        ImmutableMap.of("fieldKeyA", "fieldValueA", "fieldKeyB", "fieldValueB")));
        assertEquals(expectedFieldDefs, fieldDefs);
        
        List<DocumentExtractDefinition> extractDefs = conf.getDocumentExtractDefinitions();
        List<DocumentExtractDefinition> expectedExtractDefs = Lists.newArrayList(
                new DocumentExtractDefinition("testprefix_", "col:qual3", ValueSource.QUALIFIER, "text/html",
                        ImmutableMap.of("extractKeyA", "extractValueA", "extractKeyB", "extractValueB")));
        assertEquals(expectedExtractDefs, extractDefs);

        assertEquals(ImmutableMap.of("globalKeyA", "globalValueA", "globalKeyB", "globalValueB"), conf.getGlobalParams());
        
    }

    @Test
    public void testDefaults() throws Exception {
        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(impl, getClass().getResourceAsStream("indexerconf_defaults.xml"), Maps.<String, String>newHashMap());
        IndexerConf conf = factory.createIndexerConf();

        assertEquals("table1", conf.getTable());
        assertEquals(IndexerConf.DEFAULT_MAPPING_TYPE, conf.getMappingType());
        assertEquals(IndexerConf.DEFAULT_ROW_READ_MODE, conf.getRowReadMode());
        assertEquals(IndexerConf.DEFAULT_UNIQUE_KEY_FIELD, conf.getUniqueKeyField());
        assertNull(conf.getRowField());
        assertNull(conf.getColumnFamilyField());
        assertNull(conf.getTableNameField());
        assertEquals(IndexerConf.DEFAULT_UNIQUE_KEY_FORMATTER, conf.getUniqueKeyFormatterClass());
        assertEquals(DefaultResultToSolrMapper.class, conf.getMapperClass());

        List<FieldDefinition> fieldDefs = conf.getFieldDefinitions();
        List<FieldDefinition> expectedFieldDefs = Lists.newArrayList(
                new FieldDefinition("field1", "col:qual1", IndexerConf.DEFAULT_VALUE_SOURCE, IndexerConf.DEFAULT_FIELD_TYPE));
        assertEquals(expectedFieldDefs, fieldDefs);

        List<DocumentExtractDefinition> extractDefs = conf.getDocumentExtractDefinitions();
        List<DocumentExtractDefinition> expectedExtractDefs = Lists.newArrayList(
                new DocumentExtractDefinition(null, "col:qual2", ValueSource.VALUE, "application/octet-stream"));
        assertEquals(expectedExtractDefs, extractDefs);
    }
}
