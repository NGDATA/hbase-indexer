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

import org.apache.hadoop.conf.Configuration;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;


/**
 * Some utility methods that can be useful when writing test cases that involve the SEP.
 * 
 * This class is specifc to HBase 0.94 implementation.
 *
 */
public class SepTestUtil extends SepTestUtilCommon {
    private static final String DUMMY_TABLE_NAME = "-ROOT-";
    private static final String MBEAN_NAME = "hadoop:service=Replication,name=ReplicationSource for ";
    private static final String NAME_WILDCARD = "*";
    private static final String ATTR_NAME = "sizeOfLogQueue";

    public static void waitOnReplication(Configuration conf, long timeout) throws Exception {
        SepTestUtilCommon.waitOnReplication(conf, timeout, DUMMY_TABLE_NAME, MBEAN_NAME + NAME_WILDCARD, ATTR_NAME);
    }

    public static void waitOnReplicationPeerStopped(String peerId) {
        SepTestUtilCommon.waitOnReplicationPeerStopped(peerId);

        // At the time of this writing, HBase didn't unregister the MBean of a replication source
        try {
            MBeanServerConnection connection = java.lang.management.ManagementFactory.getPlatformMBeanServer();
            ObjectName replicationSourceMBean = new ObjectName(MBEAN_NAME + peerId);
            connection.unregisterMBean(replicationSourceMBean);
        } catch (Exception e) {
            throw new RuntimeException("Error removing replication source mean for " + peerId, e);
        }
    }

    public static void waitOnAllReplicationPeersStopped() {
        SepTestUtilCommon.waitOnAllReplicationPeersStopped();

        // At the time of this writing, HBase didn't unregister the MBean of a replication source
        try {
            MBeanServerConnection connection = java.lang.management.ManagementFactory.getPlatformMBeanServer();
            ObjectName query = new ObjectName(MBEAN_NAME + NAME_WILDCARD);
            for (ObjectName name : connection.queryNames(query, null)) {
                connection.unregisterMBean(name);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error removing replication source mbean", e);
        }
    }
}
