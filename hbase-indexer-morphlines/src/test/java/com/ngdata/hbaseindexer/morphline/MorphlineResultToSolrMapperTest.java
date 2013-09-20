/*
 * Copyright 2013 Cloudera Inc.
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
package com.ngdata.hbaseindexer.morphline;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Test;

import com.cloudera.cdk.morphline.api.Record;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.ngdata.hbaseindexer.conf.FieldDefinition;
import com.ngdata.hbaseindexer.conf.FieldDefinition.ValueSource;

import static com.ngdata.sep.impl.HBaseShims.newResult;

public class MorphlineResultToSolrMapperTest {

    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] COLUMN_FAMILY_A = Bytes.toBytes("cfA");
    private static final byte[] COLUMN_FAMILY_B = Bytes.toBytes("cfB");
    private static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
    private static final byte[] QUALIFIER_B = Bytes.toBytes("qualifierB");
    
    @Test
    public void testMap() throws Exception {
        MorphlineResultToSolrMapper resultMapper = new MorphlineResultToSolrMapper();
        resultMapper.configure(ImmutableMap.of(
            MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, "src/test/resources/test-morphlines/extractHBaseCells.conf")
            );

        KeyValue kvA = new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes(42));
        KeyValue kvB = new KeyValue(ROW, COLUMN_FAMILY_B, QUALIFIER_B, "dummy value".getBytes("UTF-8"));
        Result result = newResult(Lists.newArrayList(kvA, kvB));

        Multimap expectedMap = ImmutableMultimap.of("fieldA", 42, "fieldB", "dummy value");

        SolrInputDocument solrDocument = resultMapper.map(result);
        assertEquals(expectedMap, toRecord(solrDocument).getFields());
    }

    @Test
    public void testIsRelevantKV_WithoutWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:qualifier", ValueSource.VALUE, "byte[]");

        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier"),
                Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("wrongqualifier"), Bytes.toBytes("value"));

        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }

    @Test
    public void testIsRelevantKV_WithWildcards() {
        FieldDefinition fieldDef = new FieldDefinition("fieldA", "cf:quali*", ValueSource.QUALIFIER, "int");
        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);

        KeyValue relevantKV = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"), Bytes.toBytes("qualifier"),
                Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongFamily = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("wrongcf"),
                Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
        KeyValue notRelevantKV_WrongQualifier = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("cf"),
                Bytes.toBytes("qu wrong qualifier"), Bytes.toBytes("value"));

        assertTrue(resultMapper.isRelevantKV(relevantKV));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongFamily));
        assertFalse(resultMapper.isRelevantKV(notRelevantKV_WrongQualifier));
    }
    
    @Test
    public void testGetGet_SingleCellFieldDefinition() {
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cf:qualifier", ValueSource.VALUE, "int");
        
        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);
        Get get = resultMapper.getGet(ROW);
        
        assertArrayEquals(ROW, get.getRow());
        assertEquals(1, get.getFamilyMap().size());
        
        assertTrue(get.getFamilyMap().containsKey(Bytes.toBytes("cf")));
        NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(Bytes.toBytes("cf"));
        assertEquals(1, qualifiers.size());
        assertTrue(qualifiers.contains(Bytes.toBytes("qualifier")));
    }
    
    @Test
    public void testGetGet_WildcardFieldDefinition() {
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cf:qual*", ValueSource.VALUE, "int");
        
        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);
        Get get = resultMapper.getGet(ROW);
        
        assertArrayEquals(ROW, get.getRow());
        assertEquals(1, get.getFamilyMap().size());
        
        assertTrue(get.getFamilyMap().containsKey(Bytes.toBytes("cf")));
        NavigableSet<byte[]> qualifiers = get.getFamilyMap().get(Bytes.toBytes("cf"));
        assertNull(qualifiers);
    }
    
    @Test
    public void testContainsRequiredData_True() {
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cfA:qualifierA", ValueSource.VALUE, "int");
  
        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);
  
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes("value"))));
        
        assertTrue(resultMapper.containsRequiredData(result));
    }
    
    @Test
    public void testContainsRequiredData_False() {
        // With a wildcard we can never know if a Result contains all required data to perform indexing
        FieldDefinition fieldDef = new FieldDefinition("fieldname", "cfA:quali*", ValueSource.VALUE, "int");
        
        MorphlineResultToSolrMapper resultMapper = createMorphlineMapper(fieldDef);
        
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY_A, QUALIFIER_A, Bytes.toBytes("value"))));
        
        assertFalse(resultMapper.containsRequiredData(result));
    }

    private Record toRecord(SolrInputDocument doc) {
      Record record = new Record();
      for (Entry<String, SolrInputField> entry : doc.entrySet()) {
          record.getFields().putAll(entry.getKey(), entry.getValue().getValues());        
      }
      return record;
    }

    private MorphlineResultToSolrMapper createMorphlineMapper(FieldDefinition fieldDef) {
      MorphlineResultToSolrMapper resultMapper = new MorphlineResultToSolrMapper();
      resultMapper.configure(makeMap(
          ImmutableMap.of(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, "src/test/resources/test-morphlines/extractHBaseCellsWithVariables.conf"),
          toVariables(fieldDef)
          ));
      return resultMapper;
    }

    private Map<String, String> toVariables(FieldDefinition fieldDef) {
      return ImmutableMap.of(
          MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".INPUT_COLUMN", fieldDef.getValueExpression(),
          MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".OUTPUT_FIELD", fieldDef.getName(),
          MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".TYPE", fieldDef.getTypeName(),
          MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".SOURCE", fieldDef.getValueSource().toString().toLowerCase());
    }       
    
    private Map<String, String> makeMap(Map<String, String>...maps) {
      Map<String, String> result = new HashMap();
      for (Map<String, String> map : maps) {
          result.putAll(map);
      }
      return result;
    }
    
}
