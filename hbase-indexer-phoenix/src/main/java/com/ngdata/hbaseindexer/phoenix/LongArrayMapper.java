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
import com.google.common.primitives.Longs;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import org.apache.phoenix.schema.PDataType;

import java.sql.Array;
import java.sql.SQLException;
import java.util.Collection;

/**
 * {@link com.ngdata.hbaseindexer.parse.ByteArrayValueMapper} for {@link org.apache.phoenix.schema.PDataType#LONG_ARRAY} values.
 */
public class LongArrayMapper implements ByteArrayValueMapper {
    @Override
    public Collection<? extends Object> map(byte[] bytes) {
        Array values = (Array)PDataType.LONG_ARRAY.toObject(bytes);
        if (values == null) {
            return ImmutableList.of();
        } else {
            try {
                return Longs.asList((long[])values.getArray());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
