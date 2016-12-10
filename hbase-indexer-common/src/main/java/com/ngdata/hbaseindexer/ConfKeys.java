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
package com.ngdata.hbaseindexer;

public class ConfKeys {
    public static final String ZK_SESSION_TIMEOUT = "hbaseindexer.zookeeper.session.timeout";

    public static final String ZK_CONNECT_STRING = "hbaseindexer.zookeeper.connectstring";

    public static final String ZK_ROOT_NODE = "hbaseindexer.zookeeper.znode.parent";
    
    /**
     * Name of the Ganglia server to report to. Metrics reporting to Ganglia is only enabled
     * if a value is present under this key in the configuration.
     */
    public static final String GANGLIA_SERVER = "hbaseindexer.metrics.ganglia.server";
    
    /** Port to report to for Ganglia. */
    public static final String GANGLIA_PORT = "hbaseindexer.metrics.ganglia.port";
    
    /** Ganglia reporting interval, in seconds. */
    public static final String GANGLIA_INTERVAL = "hbaseindexer.metrics.ganglia.interval";
    
    /** HTTP header buffer size. */
    public static final String HTTP_HEADER_BUFFER_SIZE = "hbaseindexer.http.headerBufferSize";
}
