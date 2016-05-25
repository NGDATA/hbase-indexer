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
package com.ngdata.sep;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;

import java.util.ArrayList;
import java.util.List;

/**
 * Contains information about a single atomic mutation that has occurred on a row in HBase.
 */
public class SepEvent {

    private final byte[] table;
    private final byte[] row;
    private final List<KeyValue> keyValues;
    private final byte[] payload;

    /**
     * Single constructor.
     *
     * @param table The HBase table on which the event was triggered
     * @param row The row in the table where the event was triggered
     * @param keyValues The list of updates to the HBase row
     * @param payload Optional additional payload containing data about the data mutation(s)
     */
    public SepEvent(byte[] table, byte[] row, List<KeyValue> keyValues, byte[] payload) {
        this.table = table;
        this.row = row;
        this.payload = payload;
        this.keyValues = keyValues;
    }

    /**
     * Single constructor.
     *
     * @param table   The HBase table on which the event was triggered
     * @param row     The row in the table where the event was triggered
     * @param cells   The list of updates to the HBase row
     * @param payload Optional additional payload containing data about the data mutation(s)
     */
    public static SepEvent create(byte[] table, byte[] row, List<Cell> cells, byte[] payload) {
        List<KeyValue> keyValues = new ArrayList<KeyValue>(cells.size());
        for (Cell cell : cells) {
            keyValues.add((KeyValue)cell);
        }
        return new SepEvent(table, row, keyValues, payload);
    }

    /**
     * Retrieve the table where this event was triggered.
     * 
     * @return name of the HBase table
     */
    public byte[] getTable() {
        return table;
    }

    /**
     * Retrieve the row key where this event was triggered.
     * 
     * @return row key bytes
     */
    public byte[] getRow() {
        return row;
    }

    /**
     * Retrieve the payload bytes for this event. Can be null.
     * 
     * @return payload bytes, or null if not set
     */
    public byte[] getPayload() {
        return payload;
    }

    /**
     * Retrieve all grouped KeyValues that are involved in this event.
     * 
     * @return list of key values
     */
    public List<KeyValue> getKeyValues() {
        return keyValues;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        SepEvent rhs = (SepEvent)obj;
        return new EqualsBuilder().append(table, rhs.table).append(row, rhs.row).append(keyValues, rhs.keyValues).append(
                payload, rhs.payload).isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(table).append(row).append(keyValues).append(payload).toHashCode();
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE).append(table).append(row).append(keyValues).append(
                payload).toString();
    }

}
