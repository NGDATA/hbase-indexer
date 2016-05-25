/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import com.google.common.base.Preconditions;
import com.ngdata.sep.PayloadExtractor;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Extracts payload data from incoming row mutation events that have been distributed via the SEP.
 * <p>
 * Payload data can be included in a row mutation event to allow specific processing to occur as a result of the
 * mutation.
 */
public class BasePayloadExtractor implements PayloadExtractor {

    private final byte[] tableName;
    private final byte[] columnFamily;
    private final byte[] columnQualifier;

    /**
     * Construct with the table and column information from which payload data should be extracted.
     * 
     * @param tableName name of the table on which mutation events will include payload data
     * @param columnFamily column family that will include payload data
     * @param columnQualifier column qualifier of the cell from which the payload data will be extracted
     */
    public BasePayloadExtractor(byte[] tableName, byte[] columnFamily, byte[] columnQualifier) {
        Preconditions.checkNotNull(tableName, "tableName cannot be null");
        Preconditions.checkNotNull(columnFamily, "columnFamily cannot be null");
        Preconditions.checkNotNull(columnQualifier, "columnQualifier cannot be null");

        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.columnQualifier = columnQualifier;
    }

    /**
     * Extract the payload data from a KeyValue.
     * <p>
     * Data will only be extracted if it matches the configured table, column family, and column qualifiers. If no
     * payload data can be extracted, null will be returned.
     * 
     * @param tableName table to which the {@code KeyValue} is being applied
     * @param keyValue contains a (partial) row mutation which may include payload data
     * @return the extracted payload data, or null if no payload data is included in the supplied {@code KeyValue}
     */
    @Override
    public byte[] extractPayload(byte[] tableName, KeyValue keyValue) {
        if (Bytes.equals(this.tableName, tableName) && CellUtil.matchingColumn(keyValue, columnFamily, columnQualifier)) {
            return keyValue.getValue();
        } else {
            return null;
        }
        
    }

}
