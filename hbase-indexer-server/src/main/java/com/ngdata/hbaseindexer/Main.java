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

import com.ngdata.hbaseindexer.master.IndexerMaster;
import com.ngdata.hbaseindexer.model.api.IndexerProcessRegistry;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.model.impl.IndexerProcessRegistryImpl;
import com.ngdata.hbaseindexer.supervisor.IndexerRegistry;
import com.ngdata.hbaseindexer.supervisor.IndexerSupervisor;
import com.ngdata.hbaseindexer.util.zookeeper.StateWatchingZooKeeper;
import com.ngdata.sep.SepModel;
import com.ngdata.sep.impl.SepModelImpl;
import com.ngdata.sep.util.io.Closer;
import com.sun.akuma.Daemon;
import com.sun.jersey.api.core.PackagesResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;
import com.yammer.metrics.reporting.GangliaReporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.net.DNS;
import org.mortbay.jetty.Connector;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.nio.SelectChannelConnector;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;

import java.util.concurrent.TimeUnit;

public class Main {
    private final static Log log = LogFactory.getLog(Main.class);
    private HTablePool tablePool;
    private WriteableIndexerModel indexerModel;
    private SepModel sepModel;
    private IndexerMaster indexerMaster;
    private IndexerSupervisor indexerSupervisor;
    private StateWatchingZooKeeper zk;
    private Server server;

    public static void main(String[] args) {
        Daemon d = new Daemon() {
            @Override
            public void init() throws Exception {
                init("/var/run/hbase-indexer.pid");
            }
        };
        try {
        if(d.isDaemonized()) {
            // perform initialization as a daemon
            // this involves in closing file descriptors, recording PIDs, etc.
            d.init();
        } else {
            // if you are already daemonized, no point in daemonizing yourself again,
            // so do this only when you aren't daemonizing.
            if(args != null && args.length > 0 && "daemon".equals(args[0])) {
                d.daemonize();
                System.exit(0);
            }
        }
        } catch (Exception e) {
            log.error("Error setting up hbase-indexer daemon", e);
            System.exit(1);
        }

        try {
            new Main().run(args);
        } catch (Exception e) {
            log.error(e);
            System.exit(1);
        }
    }

    public void run(String[] args) throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(new ShutdownHandler()));

        Configuration conf = HBaseIndexerConfiguration.create();
        setupMetrics(conf);
        startServices(conf);
    }

    /**
     * @param conf the configuration object containing the hbase-indexer configuration, as well
     *             as the hbase/hadoop settings. Typically created using {@link HBaseIndexerConfiguration}.
     */
    public void startServices(Configuration conf) throws Exception {
        String hostname = Strings.domainNamePointerToHostName(DNS.getDefaultHost(
                conf.get("hbase.regionserver.dns.interface", "default"),
                conf.get("hbase.regionserver.dns.nameserver", "default")));

        log.debug("Using hostname " + hostname);

        String zkConnectString = conf.get(ConfKeys.ZK_CONNECT_STRING);
        int zkSessionTimeout = conf.getInt(ConfKeys.ZK_SESSION_TIMEOUT, 30000);
        zk = new StateWatchingZooKeeper(zkConnectString, zkSessionTimeout);

        tablePool = new HTablePool(conf, 10 /* TODO configurable */);

        String zkRoot = conf.get(ConfKeys.ZK_ROOT_NODE);

        indexerModel = new IndexerModelImpl(zk, zkRoot);

        sepModel = new SepModelImpl(zk, conf);

        indexerMaster = new IndexerMaster(zk, indexerModel, null, conf, zkConnectString,
                sepModel);
        indexerMaster.start();

        IndexerRegistry indexerRegistry = new IndexerRegistry();
        IndexerProcessRegistry indexerProcessRegistry = new IndexerProcessRegistryImpl(zk, conf);
        indexerSupervisor = new IndexerSupervisor(indexerModel, zk, hostname, indexerRegistry,
                indexerProcessRegistry, tablePool, conf);

        indexerSupervisor.init();
        startHttpServer();

    }

    private void startHttpServer() throws Exception {
        server = new Server();
        SelectChannelConnector selectChannelConnector = new SelectChannelConnector();
        selectChannelConnector.setPort(11060);
        server.setConnectors(new Connector[]{selectChannelConnector});

        PackagesResourceConfig packagesResourceConfig = new PackagesResourceConfig("com/ngdata/hbaseindexer/rest");

        ServletHolder servletHolder = new ServletHolder(new ServletContainer(packagesResourceConfig));
        servletHolder.setName("HBase-Indexer");


        Context context = new Context(server, "/", Context.NO_SESSIONS);
        context.addServlet(servletHolder, "/*");
        context.setContextPath("/");
        context.setAttribute("indexerModel", indexerModel);
        context.setAttribute("indexerSupervisor", indexerSupervisor);

        server.setHandler(context);
        server.start();
    }

    private void setupMetrics(Configuration conf) {
        String gangliaHost = conf.get(ConfKeys.GANGLIA_SERVER);
        if (gangliaHost != null) {
            int gangliaPort = conf.getInt(ConfKeys.GANGLIA_PORT, 8649);
            int interval = conf.getInt(ConfKeys.GANGLIA_INTERVAL, 60);
            log.info("Enabling Ganglia reporting to " + gangliaHost + ":" + gangliaPort);
            GangliaReporter.enable(interval, TimeUnit.SECONDS, gangliaHost, gangliaPort);
        }
    }

    public void stopServices() {
        log.debug("Stopping HTTP server");
        Closer.close(server);
        log.debug("Stopping indexer supervisor");
        Closer.close(indexerSupervisor);
        log.debug("Stopping indexer master");
        Closer.close(indexerMaster);
        log.debug("Stopping indexer model");
        Closer.close(indexerModel);
        log.debug("Stopping HBase table pool");
        Closer.close(tablePool);
        log.debug("Stopping ZooKeeper connection");
        Closer.close(zk);
    }

    public SepModel getSepModel() {
        return sepModel;
    }

    public WriteableIndexerModel getIndexerModel() {
        return indexerModel;
    }

    public IndexerSupervisor getIndexerSupervisor() {
        return indexerSupervisor;
    }

    public IndexerMaster getIndexerMaster() {
        return indexerMaster;
    }

    public class ShutdownHandler implements Runnable {
        @Override
        public void run() {
            stopServices();
        }
    }
}
