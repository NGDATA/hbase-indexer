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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import static com.ngdata.sep.impl.HBaseShims.newResult;

public class SingleCellExtractorTest {

    private static final byte[] ROW = Bytes.toBytes("row");
    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("col_family");
    private static final byte[] COLUMN_QUALIFIER = Bytes.toBytes("col_qualifier");

    private SingleCellExtractor extractor;

    @Before
    public void setUp() {
        extractor = new SingleCellExtractor(COLUMN_FAMILY, COLUMN_QUALIFIER);
    }

    @Test
    public void testExtract() {
        Result result = mock(Result.class);
        byte[] value = new byte[] { 1, 2, 3 };
        when(result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)).thenReturn(value);

        assertEquals(ImmutableList.of(value), extractor.extract(result));
    }

    @Test
    public void testExtract_CellNotPresent() {
        Result result = mock(Result.class);
        when(result.getValue(COLUMN_FAMILY, COLUMN_QUALIFIER)).thenReturn(null);

        assertEquals(Collections.emptyList(), extractor.extract(result));
    }

    @Test
    public void testIsApplicable_Match() {
        assertTrue(extractor.isApplicable(new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER, Bytes.toBytes("value"))));
    }

    @Test
    public void testIsApplicable_NoMatch_WrongFamily() {
        assertFalse(extractor.isApplicable(new KeyValue(ROW, Bytes.toBytes("wrong family"), COLUMN_QUALIFIER,
                Bytes.toBytes("value"))));
    }

    @Test
    public void testIsApplicable_NoMatch_WrongQualifier() {
        assertFalse(extractor.isApplicable(new KeyValue(ROW, COLUMN_FAMILY, Bytes.toBytes("wrong qualifier"),
                Bytes.toBytes("value"))));
    }

    @Test
    public void testContainsTarget_True() {
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY, COLUMN_QUALIFIER,
                Bytes.toBytes("value"))));
        assertTrue(extractor.containsTarget(result));
    }

    @Test
    public void testContainsTarget_False() {
        Result result = newResult(Lists.newArrayList(new KeyValue(ROW, COLUMN_FAMILY,
                Bytes.toBytes("wrong qualifier"), Bytes.toBytes("value"))));
        assertFalse(extractor.containsTarget(result));
    }

}
