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

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.Collection;
import java.util.Collections;

/**
 * Extracts a byte array from a single cell specified by a named column family and qualifier.
 */
public class SingleCellExtractor implements ByteArrayExtractor {

    private byte[] columnFamily;
    private byte[] columnQualifier;

    public SingleCellExtractor(byte[] columnFamily, byte[] columnQualifier) {
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
    }

    @Override
    public Collection<byte[]> extract(Result result) {
        byte[] bytes = result.getValue(columnFamily, columnQualifier);
        if (bytes == null) {
            return Collections.emptyList();
        } else {
            return Lists.newArrayList(bytes);
        }
    }
    
    @Override
    public byte[] getColumnFamily() {
        return columnFamily;
    }
    
    @Override
    public byte[] getColumnQualifier() {
        return columnQualifier;
    }
    
    @Override
    public boolean isApplicable(KeyValue keyValue) {
        return CellUtil.matchingColumn(keyValue, columnFamily, columnQualifier);
    }

    @Override
    public boolean containsTarget(Result result) {
        return result.containsColumn(columnFamily, columnQualifier);
    }

}
