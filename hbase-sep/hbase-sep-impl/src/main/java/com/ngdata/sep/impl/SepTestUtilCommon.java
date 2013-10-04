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
package com.ngdata.sep.impl;

import com.ngdata.sep.util.io.Closer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collection;
import java.util.Set;

/**
 * Some utility methods that can be useful when writing test cases that involve the SEP.
 *
 * <p>These methods assume HBase is running within the current JVM, so you will use this typically
 * together with HBase's {@code HBaseTestingUtility}.</p>
 *
 * <p>Since SEP processing is asynchronous, in test cases it can be useful to wait for
 * processing of the events to be finished, which can be done with {@link #waitOnReplication}.
 * Likewise, since the starting and stopping of the ReplicationSource threads is also asynchronous
 * (with respect to the calls on ReplicationAdmin / SepModel), there are also utility methods
 * to help waiting on that: {@link #waitOnReplicationPeerReady(String)} and
 * {@link #waitOnAllReplicationPeersStopped()}.</p>
 */
public class SepTestUtilCommon {
    /**
     * Wait for all outstanding waledit's that are currently in the hlog(s) to be replicated. Any new waledits
     * produced during the calling of this method won't be waited for.
     *
     * <p>To avoid any timing issues, after adding a replication peer you will want to call
     * {@link #waitOnReplicationPeerReady(String)} to be sure the current logs are in the queue
     * of the new peer and that the peer's mbean is registered, otherwise this method might skip
     * that peer (usually will go so fast that this problem doesn't really exist, but just to be sure).</p>
     */
    public static void waitOnReplication(Configuration conf, long timeout, String dummyTable, String mbeanName, String attrName) throws Exception {
        // Wait for the SEP to have processed all events.
        // The idea is as follows:
        //   - we want to be sure hbase replication processed all outstanding events in the hlog
        //   - therefore, roll the current hlog
        //   - if the queue of hlogs to be processed by replication contains only the current hlog (the newly
        //     rolled one), all events in the previous hlog(s) will have been processed
        //
        // It only works for one region server (which is typically the case in tests) and of course
        // assumes that new hlogs aren't being created in the meantime, but that is under all reasonable
        // circumstances the case.
        // It does ignore new edits that are happening after/during the call of this method, which is a good thing.

        // Make sure there actually is something within the hlog, otherwise it won't roll
        // This assumes the record table exits (doing the same with the .META. tables gives an exception
        // "Failed openScanner" at KeyComparator.compareWithoutRow in connect mode)
        // Writing to the -ROOT- table has the advantage that it isn't replicated, so event consumers won't
        // see any of these events.
        HTable table = new HTable(conf, dummyTable);
        Delete delete = new Delete(Bytes.toBytes("i-hope-this-row-does-not-exist"));
        table.delete(delete);

        // Roll the hlog
        rollHLog(conf);

        // Force creation of a new HLog
        delete = new Delete(Bytes.toBytes("i-hope-this-row-does-not-exist-2"));
        table.delete(delete);
        table.close();

        // Using JMX, query the size of the queue of hlogs to be processed for each replication source
        MBeanServerConnection connection = java.lang.management.ManagementFactory.getPlatformMBeanServer();
        ObjectName replicationSources = new ObjectName(mbeanName);
        Set<ObjectName> mbeans = connection.queryNames(replicationSources, null);
        long tryUntil = System.currentTimeMillis() + timeout;
        nextMBean: for (ObjectName mbean : mbeans) {
            int logQSize = Integer.MAX_VALUE;
            while (logQSize > 0 && System.currentTimeMillis() < tryUntil) {
                logQSize = ((Number)connection.getAttribute(mbean, attrName)).intValue();
                // logQSize == 0 means there is one active hlog that is polled by replication
                // and none that are queued for later processing
                // System.out.println("hlog q size is " + logQSize + " for " + mbean.toString() + " max wait left is " +
                //     (tryUntil - System.currentTimeMillis()));
                if (logQSize == 0) {
                    continue nextMBean;
                } else {
                    Thread.sleep(100);
                }
            }
            throw new Exception("Replication processing not finished with timeout " + timeout);
        }
    }

    /**
     * After adding a new replication peer, this waits for the replication source in the region server to be started.
     */
    public static void waitOnReplicationPeerReady(String peerId) {
        long tryUntil = System.currentTimeMillis() + 60000L;
        boolean waited = false;
        while (!threadExists(".replicationSource," + peerId)) {
            waited = true;
            if (System.currentTimeMillis() > tryUntil) {
                throw new RuntimeException("Replication thread for peer " + peerId + " didn't start within timeout.");
            }
            System.out.print("\nWaiting for replication source for " + peerId + " to be started...");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // I don't expect this
                throw new RuntimeException(e);
            }
        }

        if (waited) {
            System.out.println("done");
        }
    }

    /**
     * Wait for a replication peer to be stopped.
     */
    public static void waitOnReplicationPeerStopped(String peerId) {
        long tryUntil = System.currentTimeMillis() + 60000L;
        boolean waited = false;
        while (threadExists(".replicationSource," + peerId)) {
            waited = true;
            if (System.currentTimeMillis() > tryUntil) {
                throw new RuntimeException("Replication thread for peer " + peerId + " didn't stop within timeout.");
            }
            System.out.print("\nWaiting for replication source for " + peerId + " to be stopped...");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // I don't expect this
                throw new RuntimeException(e);
            }
        }

        if (waited) {
            System.out.println("done");
        }
    }

    public static void waitOnAllReplicationPeersStopped() {
        long tryUntil = System.currentTimeMillis() + 60000L;
        boolean waited = false;
        while (threadExists(".replicationSource,")) {
            waited = true;
            if (System.currentTimeMillis() > tryUntil) {
                throw new RuntimeException("Replication threads didn't stop within timeout.");
            }
            System.out.print("\nWaiting for replication sources to be stopped...");
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // I don't expect this
                throw new RuntimeException(e);
            }
        }

        if (waited) {
            System.out.println("done");
        }
    }

    private static boolean threadExists(String namepart) {
        ThreadMXBean threadmx = ManagementFactory.getThreadMXBean();
        ThreadInfo[] infos = threadmx.getThreadInfo(threadmx.getAllThreadIds());
        for (ThreadInfo info : infos) {
            if (info != null) { // see javadoc getThreadInfo (thread can have disappeared between the two calls)
                if (info.getThreadName().contains(namepart)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static void rollHLog(Configuration conf) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        try {
            Collection<ServerName> serverNames = admin.getClusterStatus().getServers();
            if (serverNames.size() != 1) {
                throw new RuntimeException("Expected exactly one region server, but got: " + serverNames.size());
            }
            admin.rollHLogWriter(serverNames.iterator().next().getServerName());
        } finally {
            Closer.close(admin);
        }
    }
}
