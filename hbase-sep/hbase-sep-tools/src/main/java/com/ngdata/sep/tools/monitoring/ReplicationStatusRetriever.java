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
package com.ngdata.sep.tools.monitoring;

import com.ngdata.sep.impl.HBaseShims;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

import com.google.common.collect.Maps;
import com.ngdata.sep.tools.monitoring.ReplicationStatus.HLogInfo;
import com.ngdata.sep.tools.monitoring.ReplicationStatus.Status;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

/**
 * Collects replication status information.
 *
 * <p>Usage: first call {@link #collectStatusFromZooKeepeer()}, then optionally call
 * {@link #addStatusFromJmx(ReplicationStatus)} for more information.
 */
public class ReplicationStatusRetriever {
    private final ZooKeeperItf zk;
    private final FileSystem fileSystem;
    private final Path hbaseRootDir;
    private final Path hbaseOldLogDir;
    public static final int HBASE_JMX_PORT = 10102;

    public ReplicationStatusRetriever(ZooKeeperItf zk, int hbaseMasterPort) throws InterruptedException, IOException, KeeperException {
        this.zk = zk;
        
        Configuration conf = getHBaseConf(zk, hbaseMasterPort);

        if (!"true".equalsIgnoreCase(conf.get("hbase.replication"))) {
            throw new RuntimeException("HBase replication is not enabled.");
        }

        
        fileSystem = FileSystem.get(conf);
        hbaseRootDir = FSUtils.getRootDir(conf);
        hbaseOldLogDir = new Path(hbaseRootDir, HConstants.HREGION_OLDLOGDIR_NAME);
    }

    private Configuration getHBaseConf(ZooKeeperItf zk, int hbaseMasterPort) throws KeeperException, InterruptedException, IOException {
        // Read the HBase/Hadoop configuration via the master web ui
        // This is debatable, but it avoids any pitfalls with conf dirs and also works with launch-test-lily
        byte[] masterServerName = removeMetaData(zk.getData("/hbase/master", false, new Stat()));
        String hbaseMasterHostName = ServerName.parseVersionedServerName(masterServerName).getHostname();
        

        String url = String.format("http://%s:%d/conf", hbaseMasterHostName, hbaseMasterPort);
        System.out.println("Reading HBase configuration from " + url);
        byte[] data = readUrl(url);

        Configuration conf = new Configuration();
        conf.addResource(new ByteArrayInputStream(data));

        return conf;
    }

    private byte[] readUrl(String url) throws IOException {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpGet httpGet = new HttpGet(url);

        HttpResponse response = httpclient.execute(httpGet);

        try {
            HttpEntity entity = response.getEntity();
            return IOUtils.toByteArray(entity.getContent());
        } finally {
            if (response.getEntity() != null) {
                EntityUtils.consume(response.getEntity());
            }
            httpGet.releaseConnection();
        }
    }

    public ReplicationStatus collectStatusFromZooKeepeer() throws Exception {
        Map<String, Map<String, Status>> statusByPeerAndServer = Maps.newHashMap();

        String regionServerPath = "/hbase/replication/rs";
        List<String> regionServers = zk.getChildren(regionServerPath, false);

        for (String server : regionServers) {
            String peersPath = regionServerPath + "/" + server;

            List<String> peers;
            try {
                peers = zk.getChildren(peersPath, false);
            } catch (KeeperException.NoNodeException e) {
                // server was removed since we called getChildren, skip it
                continue;
            }

            for (String peer : peers) {
                // The peer nodes are either real peers or recovered queues, we make no distinction for now
                String hlogsPath = peersPath + "/" + peer;

                SortedSet<String> logs;
                try {
                    // The hlogs are not correctly sorted when we get them from ZK
                    logs = new TreeSet<String>(Collections.reverseOrder());
                    logs.addAll(zk.getChildren(hlogsPath, false));
                } catch (KeeperException.NoNodeException e) {
                    // peer was removed since we called getChildren, skip it
                    continue;
                }

                for (String log : logs) {
                    Map<String, Status> statusByServer = statusByPeerAndServer.get(peer);
                    if (statusByServer == null) {
                        statusByServer = new TreeMap<String, Status>();
                        statusByPeerAndServer.put(peer, statusByServer);
                    }
                    Status status = statusByServer.get(server);
                    if (status == null) {
                        status = new Status();
                        statusByServer.put(server, status);
                    }

                    try {
                        Stat stat = new Stat();
                        byte[] data = zk.getData(hlogsPath + "/" + log, false, stat);

                        // Determine position in hlog, if already started on the hlog
                        long position = -1;
                        if (data != null && data.length > 0) {
                            data = removeMetaData(data);
                            position = Long.parseLong(new String(data, "UTF-8"));
                        }

                        HLogInfo hlogInfo = new HLogInfo(log);
                        hlogInfo.size = getLogFileSize(server, log);
                        hlogInfo.position = position;
                        status.hlogs.add(hlogInfo);
                    } catch (KeeperException.NoNodeException e) {
                        // fine, node was removed since we called getChildren
                    }
                }
            }
        }

        return new ReplicationStatus(statusByPeerAndServer);
    }

    public void addStatusFromJmx(ReplicationStatus replicationStatus) throws Exception {
        JmxConnections jmxConnections = new JmxConnections();

        for (String peerId : replicationStatus.getPeersAndRecoveredQueues()) {
            for (String server : replicationStatus.getServers(peerId)) {
                Status status = replicationStatus.getStatus(peerId, server);
                String hostName = ServerName.parseHostname(server);

                MBeanServerConnection connection = jmxConnections.getConnector(hostName, HBASE_JMX_PORT).getMBeanServerConnection();

                ObjectName replSourceBean = new ObjectName("hadoop:service=Replication,name=ReplicationSource for " + URLEncoder.encode(peerId, "UTF8"));
                try {
                    status.ageOfLastShippedOp = (Long)connection.getAttribute(replSourceBean, "ageOfLastShippedOp");
                } catch (AttributeNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                } catch (InstanceNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                }

                // The following mbean is only available when using NGDATA's ForkedReplicationSource
                ObjectName replSourceInfoBean = new ObjectName("hadoop:service=Replication,name=ReplicationSourceInfo for " + URLEncoder.encode(peerId, "UTF8"));
                try {
                    status.selectedPeerCount = (Integer)connection.getAttribute(replSourceInfoBean, "SelectedPeerCount");
                    status.timestampOfLastShippedOp = (Long)connection.getAttribute(replSourceInfoBean, "TimestampLastShippedOp");
                    status.sleepReason = (String)connection.getAttribute(replSourceInfoBean, "SleepReason");
                    status.sleepMultiplier = (Integer)connection.getAttribute(replSourceInfoBean, "SleepMultiplier");
                    status.timestampLastSleep = (Long)connection.getAttribute(replSourceInfoBean, "TimestampLastSleep");
                } catch (AttributeNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                } catch (InstanceNotFoundException e) {
                    // could be the case if the queue disappeared since we read info from ZK
                    // or the ForkedReplicationSource isn't used
                }
            }
        }

        jmxConnections.close();
    }

    /**
     *
     * @param serverName the 'unique-over-restarts' name, i.e. hostname with start code suffix
     * @param hlogName name of HLog
     */
    private long getLogFileSize(String serverName, String hlogName) throws IOException {
        Path hbaseLogDir = new Path(hbaseRootDir, HBaseShims.getHLogDirectoryName(serverName));
        Path path = new Path(hbaseLogDir, hlogName);
        try {
            FileStatus status = fileSystem.getFileStatus(path);
            return status.getLen();
        } catch (FileNotFoundException e) {
            Path oldLogPath = new Path(hbaseOldLogDir, hlogName);
            try {
                return fileSystem.getFileStatus(oldLogPath).getLen();
            } catch (FileNotFoundException e2) {
                // TODO there is still another place to look for log files, cfr dead region servers, see openReader in replicationsource
                System.err.println("HLog not found at : " + path + " or " + oldLogPath);
                return -1;
            }
        }
    }

    private static final byte MAGIC =(byte) 0XFF;
    private static final int MAGIC_SIZE = Bytes.SIZEOF_BYTE;
    private static final int ID_LENGTH_OFFSET = MAGIC_SIZE;
    private static final int ID_LENGTH_SIZE =  Bytes.SIZEOF_INT;

    /** This method was copied from RecoverableZooKeeper in the HBase 0.94 source tree. */
    public byte[] removeMetaData(byte[] data) {
        if(data == null || data.length == 0) {
            return data;
        }
        // check the magic data; to be backward compatible
        byte magic = data[0];
        if(magic != MAGIC) {
            return data;
        }

        int idLength = Bytes.toInt(data, ID_LENGTH_OFFSET);
        int dataLength = data.length-MAGIC_SIZE-ID_LENGTH_SIZE-idLength;
        int dataOffset = MAGIC_SIZE+ID_LENGTH_SIZE+idLength;

        byte[] newData = new byte[dataLength];
        System.arraycopy(data, dataOffset, newData, 0, dataLength);
        return newData;
    }
}
