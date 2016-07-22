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

import com.google.common.collect.Iterables;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import com.ngdata.hbaseindexer.uniquekey.UniqueKeyFormatter;
import org.apache.hadoop.hbase.KeyValue;

import java.util.Arrays;


abstract class AbstractUniqueKeyFormatter implements UniqueKeyFormatter {

    private final ByteArrayValueMapper byteArrayValueMapper;


    protected AbstractUniqueKeyFormatter(ByteArrayValueMapper byteArrayValueMapper) {
        this.byteArrayValueMapper = byteArrayValueMapper;
    }

    @Override
    public final String formatRow(byte[] row) {
        Object formatted = Iterables.getFirst(byteArrayValueMapper.map(row), null);
        if (formatted == null) {
            throw new IllegalStateException("Formatted row was null from " + Arrays.toString(row));
        }
        return String.valueOf(formatted);
    }

    @Override
    public String formatFamily(byte[] family) {
        throw new UnsupportedOperationException("formatFamily is not supported");
    }

    @Override
    public String formatKeyValue(KeyValue keyValue) {
        throw new UnsupportedOperationException("formatKeyValue is not supported");
    }

    @Override
    public byte[] unformatRow(String keyString) {
        throw new UnsupportedOperationException("Unformatting is not supported");
    }

    @Override
    public byte[] unformatFamily(String familyString) {
        throw new UnsupportedOperationException("Unformatting is not supported");
    }

    @Override
    public KeyValue unformatKeyValue(String keyValueString) {
        throw new UnsupportedOperationException("Unformatting is not supported");
    }
}
