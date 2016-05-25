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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;

/**
 * Abstract base class for prefix-matching {@link ByteArrayExtractor}s.
 */
public abstract class AbstractPrefixMatchingExtractor implements ByteArrayExtractor {

    private byte[] columnFamily;
    private byte[] prefix;

    AbstractPrefixMatchingExtractor(byte[] columnFamily, byte[] prefix) {
        this.columnFamily = columnFamily;
        this.prefix = prefix;
    }

    @VisibleForTesting
    byte[] getPrefix() {
        return prefix;
    }

    /**
     * Extract a value from a {@code KeyValue}. What portion of the KeyValue to be fetched is implementation-specific.
     * 
     * @param keyValue source for the extraction
     * @return extracted value, or null if no value can be extracted
     */
    protected abstract byte[] extractInternal(byte[] qualifier, byte[] value);

    @Override
    public Collection<byte[]> extract(Result result) {
        List<byte[]> values = Lists.newArrayList();

        NavigableMap<byte[], byte[]> qualifiersToValues = result.getFamilyMap(columnFamily);
        if (qualifiersToValues != null) {
            for (byte[] qualifier : qualifiersToValues.navigableKeySet().tailSet(prefix)) {
                if (Bytes.startsWith(qualifier, prefix)) {
                    values.add(extractInternal(qualifier, qualifiersToValues.get(qualifier)));
                } else {
                    break;
                }
            }
        }
        return values;
    }
    
    @Override
    public boolean isApplicable(KeyValue keyValue) {
        return CellUtil.matchingFamily(keyValue, columnFamily) && Bytes.startsWith(keyValue.getQualifier(), prefix);
    }

    @Override
    public byte[] getColumnFamily() {
        return columnFamily;
    }

    @Override
    public byte[] getColumnQualifier() {
        return null;
    }
    
    @Override
    public boolean containsTarget(Result result) {
        // We're matching multiple potential inputs, so we can never be sure if all possibly-matching situations
        // are included in the given result, so we just always return false.
        return false;
    }
}
