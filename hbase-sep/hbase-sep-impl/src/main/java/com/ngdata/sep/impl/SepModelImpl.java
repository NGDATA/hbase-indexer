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
import java.util.UUID;

import com.ngdata.sep.util.zookeeper.ZkUtil;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;

import com.ngdata.sep.util.io.Closer;

import com.ngdata.sep.SepModel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

public class SepModelImpl implements SepModel {
    
    // Replace '-' with unicode "CANADIAN SYLLABICS HYPHEN" character in zookeeper to avoid issues
    // with HBase replication naming conventions
    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';
    
    private final ZooKeeperItf zk;
    private final Configuration hbaseConf;
    private final String baseZkPath;
    private final String zkQuorumString;
    private final int zkClientPort;
    private Log log = LogFactory.getLog(getClass());

    public SepModelImpl(ZooKeeperItf zk, Configuration hbaseConf) {
        
        this.zkQuorumString = hbaseConf.get("hbase.zookeeper.quorum");
        if (zkQuorumString == null) {
            throw new IllegalStateException("hbase.zookeeper.quorum not supplied in configuration");
        }
        if (zkQuorumString.contains(":")) {
            throw new IllegalStateException("hbase.zookeeper.quorum should not include port number, got " + zkQuorumString);
        }
        try {
            this.zkClientPort = Integer.parseInt(hbaseConf.get("hbase.zookeeper.property.clientPort"));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Non-numeric zookeeper client port", e);
        }
        
        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.baseZkPath = hbaseConf.get(ZK_ROOT_NODE_CONF_KEY, DEFAULT_ZK_ROOT_NODE);
    }

    @Override
    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (!addSubscriptionSilent(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }
    }

    @Override
    public boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(hbaseConf);
        try {
            String internalName = toInternalSubscriptionName(name);
            if (replicationAdmin.listPeers().containsKey(internalName)) {
                return false;
            }

            String basePath = baseZkPath + "/" + internalName;
            UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
            ZkUtil.createPath(zk, basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
            ZkUtil.createPath(zk, basePath + "/rs");


            try {
                replicationAdmin.addPeer(internalName, zkQuorumString + ":" + zkClientPort + ":" + basePath);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot add existing peer")) {
                    return false;
                }
                throw e;
            } catch (Exception e) {
                // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                if (e instanceof InterruptedException) {
                    throw (InterruptedException)e;
                } else if (e instanceof KeeperException) {
                    throw (KeeperException)e;
                } else {
                    throw new IOException(e);
                }
            }

            return true;
        } finally {
            Closer.close(replicationAdmin);
        }
    }

    @Override
    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    @Override
    public boolean removeSubscriptionSilent(String name) throws IOException {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(hbaseConf);
        try {
            String internalName = toInternalSubscriptionName(name);
            if (!replicationAdmin.listPeers().containsKey(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                try {
                    replicationAdmin.removePeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            String basePath = baseZkPath + "/" + internalName;
            try {
                ZkUtil.deleteNode(zk, basePath + "/hbaseid");
                for (String child : zk.getChildren(basePath + "/rs", false)) {
                    ZkUtil.deleteNode(zk, basePath + "/rs/" + child);
                }
                ZkUtil.deleteNode(zk, basePath + "/rs");
                ZkUtil.deleteNode(zk, basePath);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (KeeperException ke) {
                log.error("Cleanup in zookeeper failed on " + basePath, ke);
            }
            return true;
        } finally {
            Closer.close(replicationAdmin);
        }
    }

    @Override
    public boolean hasSubscription(String name) throws IOException {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(hbaseConf);
        try {
            String internalName = toInternalSubscriptionName(name);
            return replicationAdmin.listPeers().containsKey(internalName);
        } finally {
            Closer.close(replicationAdmin);
        }
    }
    
    static String toInternalSubscriptionName(String subscriptionName) {
        if (subscriptionName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U1400");
        }
        return subscriptionName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }

    static String toExternalSubscriptionName(String subscriptionName) {
        return subscriptionName.replace(INTERNAL_HYPHEN_REPLACEMENT, '-');
    }
}
