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
package com.ngdata.hbaseindexer.indexer;

import com.google.common.collect.Lists;
import com.ngdata.sep.SepEvent;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import java.util.Collections;
import java.util.List;

/**
 * {@code RowData} implementation that wraps an incoming SepEvent for
 * NRT indexing.
 */
public class SepEventRowData implements RowData {

    private final SepEvent sepEvent;
    
    public SepEventRowData(SepEvent sepEvent) {
        this.sepEvent = sepEvent;
    }
    
    @Override
    public byte[] getRow() {
        return sepEvent.getRow();
    }

    @Override
    public byte[] getTable() {
        return sepEvent.getTable();
    }

    @Override
    public List<KeyValue> getKeyValues() {
        return sepEvent.getKeyValues();
    }

    /**
     * Makes a HBase Result object based on the KeyValue's from the SEP event. Usually, this will only be used in
     * situations where only new data is written (or updates are complete row updates), so we don't expect any
     * delete-type key-values, but just to be sure we filter them out.
     */
    @Override
    public Result toResult() {
        
        List<KeyValue> filteredKeyValues = Lists.newArrayListWithCapacity(sepEvent.getKeyValues().size());
        
        for (KeyValue kv : getKeyValues()) {
            if (!CellUtil.isDelete(kv) && !CellUtil.isDeleteFamily(kv)) {
                filteredKeyValues.add(kv);
            }
        }

        // A Result object requires that the KeyValues are sorted (e.g., it does binary search on them)
        Collections.sort(filteredKeyValues, KeyValue.COMPARATOR);
        return new Result(filteredKeyValues);
    }

}
