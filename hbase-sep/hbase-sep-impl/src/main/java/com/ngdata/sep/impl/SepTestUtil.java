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

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

/**
 * Some utility methods that can be useful when writing test cases that involve the SEP.
 *
 * <p>These methods assume HBase is running within the current JVM, so you will use this typically
 * together with HBase's {@code HBaseTestingUtility}.</p>
 *
 * <p>Since starting and stopping of the ReplicationSource threads is asynchronous
 * (with respect to the calls on ReplicationAdmin / SepModel), there are utility methods
 * to help waiting on that: {@link #waitOnReplicationPeerReady(String)} and
 * {@link #waitOnAllReplicationPeersStopped()}.</p>
 */
public class SepTestUtil {
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

}
