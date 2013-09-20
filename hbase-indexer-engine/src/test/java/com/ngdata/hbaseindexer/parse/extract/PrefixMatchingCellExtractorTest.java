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
package com.ngdata.hbaseindexer.parse.extract;

import static com.ngdata.hbaseindexer.parse.extract.ExtractTestUtil.assertByteArraysEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static com.ngdata.sep.impl.HBaseShims.newResult;

public class PrefixMatchingCellExtractorTest {

    private static final byte[] ROW = new byte[] { 1, 2, 3 };
    private static final byte[] COLFAM_A = Bytes.toBytes("A");
    private static final byte[] COLFAM_B = Bytes.toBytes("B");
    private static final byte[] QUALIFIER_A1 = Bytes.toBytes("A1");
    private static final byte[] QUALIFIER_A2 = Bytes.toBytes("A2");
    private static final byte[] QUALIFIER_B1 = Bytes.toBytes("B1");
    private static final byte[] VALUE_A1 = Bytes.toBytes("value a1");
    private static final byte[] VALUE_A2 = Bytes.toBytes("value a2");
    private static final byte[] VALUE_B1 = Bytes.toBytes("value b1");

    private Result result;

    @Before
    public void setUp() {
        KeyValue kvA1 = new KeyValue(ROW, COLFAM_A, QUALIFIER_A1, VALUE_A1);
        KeyValue kvA2 = new KeyValue(ROW, COLFAM_A, QUALIFIER_A2, VALUE_A2);
        KeyValue kvB1 = new KeyValue(ROW, COLFAM_B, QUALIFIER_B1, VALUE_B1);

        result = newResult(Lists.newArrayList(kvA1, kvA2, kvB1));
    }

    @Test
    public void testExtract() {
        ByteArrayExtractor extractor = new PrefixMatchingCellExtractor(COLFAM_A, Bytes.toBytes("A"));
        assertByteArraysEquals(Lists.newArrayList(VALUE_A1, VALUE_A2), extractor.extract(result));
    }

    @Test
    public void testExtract_EmptyPrefix() {
        ByteArrayExtractor extractor = new PrefixMatchingCellExtractor(COLFAM_A, new byte[0]);
        assertByteArraysEquals(Lists.newArrayList(VALUE_A1, VALUE_A2), extractor.extract(result));
    }

    @Test
    public void testExtract_FullCellName() {
        ByteArrayExtractor extractor = new PrefixMatchingCellExtractor(COLFAM_A, Bytes.toBytes("A1"));
        assertByteArraysEquals(Lists.newArrayList(VALUE_A1), extractor.extract(result));
    }

    @Test
    public void testExtract_NoMatches() {
        ByteArrayExtractor extractor = new PrefixMatchingCellExtractor(COLFAM_A, Bytes.toBytes("doesnt exist"));
        assertTrue(extractor.extract(result).isEmpty());
    }
    
    @Test
    public void testGetColumnQualifier() {
        assertNull(new PrefixMatchingCellExtractor(COLFAM_A, Bytes.toBytes("A1")).getColumnQualifier());
    }

}
