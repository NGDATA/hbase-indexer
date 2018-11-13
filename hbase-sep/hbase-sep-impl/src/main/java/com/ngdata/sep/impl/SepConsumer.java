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
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.ngdata.sep.EventListener;
import com.ngdata.sep.PayloadExtractor;
import com.ngdata.sep.SepEvent;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.util.concurrent.WaitPolicy;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * SepConsumer consumes the events for a certain SEP subscription and dispatches
 * them to an EventListener (optionally multi-threaded). Multiple SepConsumer's
 * can be started that take events from the same subscription: each consumer
 * will receive a subset of the events.
 *
 * <p>On a more technical level, SepConsumer is the remote process (the "fake
 * hbase regionserver") to which the regionservers in the hbase master cluster
 * connect to replicate log entries.</p>
 */
public class SepConsumer extends BaseHRegionServer {
    private final String subscriptionId;
    private long subscriptionTimestamp;
    private EventListener listener;
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private RpcServer rpcServer;
    private ServerName serverName;
    private ZooKeeperWatcher zkWatcher;
    private SepMetrics sepMetrics;
    private final PayloadExtractor payloadExtractor;
    private String zkNodePath;
    private List<ThreadPoolExecutor> executors;
    boolean running = false;
    private Log log = LogFactory.getLog(getClass());

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not
     *                              inactive)
     * @param listener              listeners that will process the events
     * @param threadCnt             number of worker threads that will handle incoming SEP events
     * @param hostName              hostname, this is published in ZooKeeper and will be used by HBase to connect to this
     *                              consumer, so there should be only one SepConsumer instance for a (subscriptionId, host)
     *                              combination, and the hostname should definitely not be "localhost". This is also the hostname
     *                              that the RPC will bind to.
     */
    public SepConsumer(String subscriptionId, long subscriptionTimestamp, EventListener listener, int threadCnt,
            String hostName, ZooKeeperItf zk, Configuration hbaseConf) throws IOException, InterruptedException {
        this(subscriptionId, subscriptionTimestamp, listener, threadCnt, hostName, zk, hbaseConf, null);
    }

    /**
     * @param subscriptionTimestamp timestamp of when the index subscription became active (or more accurately, not
     *                              inactive)
     * @param listener              listeners that will process the events
     * @param threadCnt             number of worker threads that will handle incoming SEP events
     * @param hostName              hostname to bind to
     * @param payloadExtractor      extracts payloads to include in SepEvents
     */
    public SepConsumer(String subscriptionId, long subscriptionTimestamp, EventListener listener, int threadCnt,
            String hostName, ZooKeeperItf zk, Configuration hbaseConf, PayloadExtractor payloadExtractor) throws IOException, InterruptedException {
        Preconditions.checkArgument(threadCnt > 0, "Thread count must be > 0");
        this.subscriptionId = SepModelImpl.toInternalSubscriptionName(subscriptionId);
        this.subscriptionTimestamp = subscriptionTimestamp;
        this.listener = listener;
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.sepMetrics = new SepMetrics(subscriptionId);
        this.payloadExtractor = payloadExtractor;
        this.executors = Lists.newArrayListWithCapacity(threadCnt);

        InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
        if (initialIsa.getAddress() == null) {
            throw new IllegalArgumentException("Failed resolve of " + initialIsa);
        }
        String name = "regionserver/" + initialIsa.toString();
        this.rpcServer = new RpcServer(this, name, getServices(),
        /*HBaseRPCErrorHandler.class, OnlineRegions.class},*/
                initialIsa, // BindAddress is IP we got for this server.
                //hbaseConf.getInt("hbase.regionserver.handler.count", 10),
                //hbaseConf.getInt("hbase.regionserver.metahandler.count", 10),
                hbaseConf,
                new FifoRpcScheduler(hbaseConf, hbaseConf.getInt("hbase.regionserver.handler.count", 10)));
          /*
          new SimpleRpcScheduler(
            hbaseConf,
            hbaseConf.getInt(HConstants.REGION_SERVER_HANDLER_COUNT, HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT),
            hbaseConf.getInt("hbase.regionserver.metahandler.count", 10),
            hbaseConf.getInt("hbase.regionserver.handler.count", 10),
            this,
            HConstants.QOS_THRESHOLD)
          );
          */
        this.serverName = ServerName.valueOf(hostName, rpcServer.getListenerAddress().getPort(), System.currentTimeMillis());
        this.zkWatcher = new ZooKeeperWatcher(hbaseConf, this.serverName.toString(), null);

        // login the zookeeper client principal (if using security)
        ZKUtil.loginClient(hbaseConf, "hbase.zookeeper.client.keytab.file",
                "hbase.zookeeper.client.kerberos.principal", hostName);

        // login the server principal (if using secure Hadoop)
        User.login(hbaseConf, "hbase.regionserver.keytab.file",
                "hbase.regionserver.kerberos.principal", hostName);

        for (int i = 0; i < threadCnt; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
    }

    public void start() throws IOException, InterruptedException, KeeperException {

        rpcServer.start();

        // Publish our existence in ZooKeeper
        zkNodePath = hbaseConf.get(SepModel.ZK_ROOT_NODE_CONF_KEY, SepModel.DEFAULT_ZK_ROOT_NODE)
                + "/" + subscriptionId + "/rs/" + serverName.getServerName();
        zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        this.running = true;
    }

    private List<RpcServer.BlockingServiceAndInterface> getServices() {
        List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<RpcServer.BlockingServiceAndInterface>(1);
        bssi.add(new RpcServer.BlockingServiceAndInterface(
                AdminProtos.AdminService.newReflectiveBlockingService(this),
                AdminProtos.AdminService.BlockingInterface.class));
        return bssi;
    }

    public void stop() {
        Closer.close(zkWatcher);
        if (running) {
            running = false;
            Closer.close(rpcServer);
            try {
                // This ZK node will likely already be gone if the index has been removed
                // from ZK, but we'll try to remove it here to be sure
                zk.delete(zkNodePath, -1);
            } catch (Exception e) {
                log.debug("Exception while removing zookeeper node", e);
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        sepMetrics.shutdown();
        for (ThreadPoolExecutor executor : executors) {
            executor.shutdown();
        }
    }

    public boolean isRunning() {
        return running;
    }

    @Override
    public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
            final AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
        try {

            // TODO Recording of last processed timestamp won't work if two batches of log entries are sent out of order
            long lastProcessedTimestamp = -1;

            SepEventExecutor eventExecutor = new SepEventExecutor(listener, executors, 100, sepMetrics);

            List<AdminProtos.WALEntry> entries = request.getEntryList();
            CellScanner cells = ((HBaseRpcController)controller).cellScanner();

            for (final AdminProtos.WALEntry entry : entries) {
                TableName tableName = (entry.getKey().getWriteTime() < subscriptionTimestamp) ? null :
                        TableName.valueOf(entry.getKey().getTableName().toByteArray());
                Multimap<ByteBuffer, Cell> keyValuesPerRowKey = ArrayListMultimap.create();
                final Map<ByteBuffer, byte[]> payloadPerRowKey = Maps.newHashMap();
                int count = entry.getAssociatedCellCount();
                for (int i = 0; i < count; i++) {
                    if (!cells.advance()) {
                        throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
                    }

                    // this signals to us that we simply need to skip over count of cells
                    if (tableName == null) {
                        continue;
                    }

                    Cell cell = cells.current();
                    ByteBuffer rowKey = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                    byte[] payload;
                    KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
                    if (payloadExtractor != null && (payload = payloadExtractor.extractPayload(tableName.toBytes(), kv)) != null) {
                        if (payloadPerRowKey.containsKey(rowKey)) {
                            log.error("Multiple payloads encountered for row " + Bytes.toStringBinary(rowKey)
                                    + ", choosing " + Bytes.toStringBinary(payloadPerRowKey.get(rowKey)));
                        } else {
                            payloadPerRowKey.put(rowKey, payload);
                        }
                    }
                    keyValuesPerRowKey.put(rowKey, kv);
                }

                for (final ByteBuffer rowKeyBuffer : keyValuesPerRowKey.keySet()) {
                    final List<Cell> keyValues = (List<Cell>) keyValuesPerRowKey.get(rowKeyBuffer);

                    final SepEvent sepEvent = new SepEvent(tableName.toBytes(), CellUtil.cloneRow(keyValues.get(0)), keyValues,
                            payloadPerRowKey.get(rowKeyBuffer));
                    eventExecutor.scheduleSepEvent(sepEvent);
                    lastProcessedTimestamp = Math.max(lastProcessedTimestamp, entry.getKey().getWriteTime());
                }

            }
            List<Future<?>> futures = eventExecutor.flush();
            waitOnSepEventCompletion(futures);

            if (lastProcessedTimestamp > 0) {
                sepMetrics.reportSepTimestamp(lastProcessedTimestamp);
            }
            return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
        } catch (IOException ie) {
            throw new ServiceException(ie);
        }
    }

    private void waitOnSepEventCompletion(List<Future<?>> futures) throws IOException {
        // We should wait for all operations to finish before returning, because otherwise HBase might
        // deliver a next batch from the same HLog to a different server. This becomes even more important
        // if an exception has been thrown in the batch, as waiting for all futures increases the back-off that
        // occurs before the next attempt
        List<Exception> exceptionsThrown = Lists.newArrayList();
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted in processing events.", e);
            } catch (Exception e) {
                // While we throw the error higher up, to HBase, where it will also be logged, apparently the
                // nested exceptions don't survive somewhere, therefore log it client-side as well.
                log.warn("Error processing a batch of SEP events, the error will be forwarded to HBase for retry", e);
                exceptionsThrown.add(e);
            }
        }

        if (!exceptionsThrown.isEmpty()) {
            log.error("Encountered exceptions on " + exceptionsThrown.size() + " batches (out of " + futures.size()
                    + " total batches)");
            throw new RuntimeException(exceptionsThrown.get(0));
        }
    }

    @Override
    public Configuration getConfiguration() {
        return hbaseConf;
    }

    @Override
    public ServerName getServerName() {
        return serverName;
    }

    @Override
    public ZooKeeperWatcher getZooKeeper() {
        return zkWatcher;
    }

}
