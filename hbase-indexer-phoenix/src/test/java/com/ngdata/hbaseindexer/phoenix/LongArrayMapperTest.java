/*
 * Copyright 2014 NGDATA nv
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ngdata.hbaseindexer.phoenix;

import com.google.common.collect.ImmutableList;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.util.ByteUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LongArrayMapperTest {
    private LongArrayMapper mapper = new LongArrayMapper();

    @Test
    public void testMap() {
        PhoenixArray array = new PhoenixArray(PDataType.LONG, new Long[] { 1L, 10L, 100L });
        assertEquals(
                ImmutableList.of(1L, 10L, 100L),
                mapper.map(PDataType.LONG_ARRAY.toBytes(array)));
    }

    @Test
    public void testMapNull() {
        assertEquals(
                ImmutableList.of(),
                mapper.map(ByteUtil.EMPTY_BYTE_ARRAY));
    }
}
