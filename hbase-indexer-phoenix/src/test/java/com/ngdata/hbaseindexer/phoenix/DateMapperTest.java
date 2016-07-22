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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DateMapperTest {

    private DateMapper mapper = new DateMapper();

    private DateTime now = new DateTime(DateTimeZone.UTC);

    @Test
    public void testMap() {
        assertEquals(
                ImmutableList.of(now.toDate()),
                mapper.map(PDataType.DATE.toBytes(now.toDate())));
    }

    @Test
    public void testMap_EndOfCompositeValue() {
        byte[] buffer = new byte[PDataType.DATE.getByteSize() + 2];
        PDataType.DATE.toBytes(now.toDate(), buffer, 2);
        assertEquals(
                ImmutableList.of(now.toDate()),
                mapper.map(buffer));
    }
}
