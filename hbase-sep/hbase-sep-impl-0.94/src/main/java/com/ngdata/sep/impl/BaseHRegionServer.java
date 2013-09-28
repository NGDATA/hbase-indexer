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

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.MultiAction;
import org.apache.hadoop.hbase.client.MultiResponse;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.WritableByteArrayComparable;
import org.apache.hadoop.hbase.io.hfile.BlockCacheColumnFamilySummary;
import org.apache.hadoop.hbase.ipc.HRegionInterface;
import org.apache.hadoop.hbase.ipc.ProtocolSignature;
import org.apache.hadoop.hbase.regionserver.RegionOpeningState;
import org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Pair;

/**
 * Empty implementation of HRegionInterface, except for {@link #getProtocolVersion} and
 * {@link #getProtocolVersion}.
 */
public class BaseHRegionServer implements HRegionInterface {
    
    // Constant dummy value that is returned from getHServerInfo.
    private static final HServerInfo HSERVER_INFO = new HServerInfo();
    
    @Override
    public long getProtocolVersion(final String protocol, final long clientVersion) throws IOException {
        if (protocol.equals(HRegionInterface.class.getName())) {
          return HRegionInterface.VERSION;
        }
        throw new IOException("Unknown protocol: " + protocol);
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long version, int clientMethodsHashCode)
            throws IOException {
        if (protocol.equals(HRegionInterface.class.getName())) {
          return new ProtocolSignature(HRegionInterface.VERSION, null);
        }
        throw new IOException("Unknown protocol: " + protocol);
    }

    @Override
    public HRegionInfo getRegionInfo(byte[] regionName) throws NotServingRegionException, ConnectException,
            IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result getClosestRowBefore(byte[] regionName, byte[] row, byte[] family) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result get(byte[] regionName, Get get) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean exists(byte[] regionName, Get get) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void put(byte[] regionName, Put put) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int put(byte[] regionName, List<Put> puts) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void delete(byte[] regionName, Delete delete) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public int delete(byte[] regionName, List<Delete> deletes) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value,
            Put put) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, byte[] value,
            Delete delete) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long incrementColumnValue(byte[] regionName, byte[] row, byte[] family, byte[] qualifier, long amount,
            boolean writeToWAL) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result increment(byte[] regionName, Increment increment) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long openScanner(byte[] regionName, Scan scan) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result next(long scannerId) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result[] next(long scannerId, int numberOfRows) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void close(long scannerId) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long lockRow(byte[] regionName, byte[] row) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void unlockRow(byte[] regionName, long lockId) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<HRegionInfo> getOnlineRegions() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public HServerInfo getHServerInfo() throws IOException {
        // Need to return something here as this method is used to ensure that a replication peer is up
        return HSERVER_INFO;
    }

    @Override
    public <R> MultiResponse multi(MultiAction<R> multi) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean bulkLoadHFiles(List<Pair<byte[], String>> familyPaths, byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RegionOpeningState openRegion(HRegionInfo region) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public RegionOpeningState openRegion(HRegionInfo region, int versionOfOfflineNode) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void openRegions(List<HRegionInfo> regions) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean closeRegion(HRegionInfo region) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean closeRegion(HRegionInfo region, int versionOfClosingNode) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean closeRegion(HRegionInfo region, boolean zk) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean closeRegion(byte[] encodedRegionName, boolean zk) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void flushRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void splitRegion(HRegionInfo regionInfo) throws NotServingRegionException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void splitRegion(HRegionInfo regionInfo, byte[] splitPoint) throws NotServingRegionException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void compactRegion(HRegionInfo regionInfo, boolean major) throws NotServingRegionException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void replicateLogEntries(HLog.Entry[] entries) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public ExecResult execCoprocessor(byte[] regionName, Exec call) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean checkAndPut(byte[] regionName, byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Put put) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean checkAndDelete(byte[] regionName, byte[] row, byte[] family, byte[] qualifier,
            CompareFilter.CompareOp compareOp, WritableByteArrayComparable comparator, Delete delete)
            throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<BlockCacheColumnFamilySummary> getBlockCacheColumnFamilySummaries() throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public byte[][] rollHLogWriter() throws IOException, FailedLogCloseException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void stop(String why) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void abort(String why, Throwable e) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isAborted() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public boolean isStopped() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void flushRegion(byte[] regionName) throws IllegalArgumentException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void flushRegion(byte[] regionName, long ifOlderThanTS) throws IllegalArgumentException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public long getLastFlushTime(byte[] regionName) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> getStoreFileList(byte[] regionName, byte[] columnFamily) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> getStoreFileList(byte[] regionName, byte[][] columnFamilies) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<String> getStoreFileList(byte[] regionName) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void mutateRow(byte[] regionName, RowMutations rm) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Result append(byte[] regionName, Append append) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getCompactionState(byte[] regionName) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void compactRegion(HRegionInfo regionInfo, boolean major, byte[] columnFamily)
            throws NotServingRegionException, IOException {
        throw new UnsupportedOperationException("Not implemented");
    }
}
