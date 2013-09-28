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
package com.ngdata.hbaseindexer.supervisor;

import com.ngdata.sep.impl.HBaseShims; 

import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.INDEXER_ADDED;
import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.INDEXER_DELETED;
import static com.ngdata.hbaseindexer.model.api.IndexerModelEventType.INDEXER_UPDATED;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.xml.parsers.ParserConfigurationException;

import com.google.common.collect.Maps;

import com.google.common.base.Objects;
import com.ngdata.hbaseindexer.ConfigureUtil;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.conf.XmlIndexerConfReader;
import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import com.ngdata.hbaseindexer.model.api.IndexerModelEvent;
import com.ngdata.hbaseindexer.model.api.IndexerModelListener;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.model.api.IndexerProcessRegistry;
import com.ngdata.hbaseindexer.parse.DefaultResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.util.solr.SolrConfigLoader;
import com.ngdata.sep.impl.SepConsumer;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
// import org.apache.hadoop.hbase.EmptyWatcher;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.util.SystemIdResolver;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * Responsible for starting, stopping and restarting {@link Indexer}s for the indexers defined in the
 * {@link IndexerModel}.
 */
public class IndexerSupervisor {
    private final IndexerModel indexerModel;

    private final ZooKeeperItf zk;

    private final String hostName;

    private final IndexerModelListener listener = new MyListener();

    private final Map<String, IndexerHandle> indexers = new HashMap<String, IndexerHandle>();
    
    private final Object indexersLock = new Object();

    private final BlockingQueue<IndexerModelEvent> eventQueue = new LinkedBlockingQueue<IndexerModelEvent>();

    private EventWorker eventWorker;

    private Thread eventWorkerThread;

    private HttpClient httpClient;

    private ThreadSafeClientConnManager connectionManager;

    private final IndexerRegistry indexerRegistry;
    
    private final IndexerProcessRegistry indexerProcessRegistry;
    
    private final Map<String,String> indexerProcessIds;

    private final HTablePool htablePool;

    private final Configuration hbaseConf;
    
    private final Log log = LogFactory.getLog(getClass());

    /**
     * Total number of IndexerModel events processed (useful in test cases).
     */
    private final AtomicInteger eventCount = new AtomicInteger();

    public IndexerSupervisor(IndexerModel indexerModel, ZooKeeperItf zk, String hostName,
            IndexerRegistry indexerRegistry, IndexerProcessRegistry indexerProcessRegistry,
            HTablePool htablePool, Configuration hbaseConf)
            throws IOException, InterruptedException {
        this.indexerModel = indexerModel;
        this.zk = zk;
        this.hostName = hostName;
        this.indexerRegistry = indexerRegistry;
        this.indexerProcessRegistry = indexerProcessRegistry;
        this.indexerProcessIds = Maps.newHashMap();
        this.htablePool = htablePool;
        this.hbaseConf = hbaseConf;
    }

    @PostConstruct
    public void init() {
        connectionManager = new ThreadSafeClientConnManager();
        httpClient = new DefaultHttpClient(connectionManager);

        eventWorker = new EventWorker();
        eventWorkerThread = new Thread(eventWorker, "IndexerWorkerEventWorker");
        eventWorkerThread.start();

        synchronized (indexersLock) {
            Collection<IndexerDefinition> indexerDefs = indexerModel.getIndexers(listener);

            for (IndexerDefinition indexerDef : indexerDefs) {
                if (shouldRunIndexer(indexerDef)) {
                    startIndexer(indexerDef);
                }
            }
        }
    }

    @PreDestroy
    public void stop() {
        eventWorker.stop();
        eventWorkerThread.interrupt();
        try {
            eventWorkerThread.join();
        } catch (InterruptedException e) {
            log.info("Interrupted while joining eventWorkerThread.");
        }

        for (IndexerHandle handle : indexers.values()) {
            try {
                handle.stop();
            } catch (InterruptedException e) {
                // Continue the stop procedure
            }
        }

        connectionManager.shutdown();
    }

    public int getEventCount() {
        return eventCount.get();
    }

    public Set<String> getRunningIndexers() {
        return indexers.keySet();
    }

    private SolrServer getSolrServer(IndexerDefinition indexerDef) throws MalformedURLException {
        if (!"solr".equals(indexerDef.getConnectionType())) {
            throw new RuntimeException("Only indexers with connectionType=solr are supported, but found type: '"
                    + indexerDef.getConnectionType() + "'.");
        }

        String solrMode = indexerDef.getConnectionParams().get(SolrConnectionParams.MODE);
        if (solrMode == null || solrMode.equals("cloud")) {
            String solrZk = indexerDef.getConnectionParams().get(SolrConnectionParams.ZOOKEEPER);
            CloudSolrServer solr = new CloudSolrServer(solrZk);
            String collection = indexerDef.getConnectionParams().get(SolrConnectionParams.COLLECTION);
            solr.setDefaultCollection(collection);
            return solr;
        } else {
            throw new RuntimeException("Only indexers with connection parameter solr.mode=cloud are supported," +
                    " found : '" + solrMode + "'.");
        }
    }
    
    private void startIndexer(IndexerDefinition indexerDef) {
        IndexerHandle handle = null;
        SolrServer solr = null;
        
     
        String indexerProcessId = null;
        try {
            
            // If this is an update and the indexer failed to start, it's still in the registry as a
            // failed process
            if (indexerProcessIds.containsKey(indexerDef.getName())) {
                indexerProcessRegistry.unregisterIndexerProcess(indexerProcessIds.remove(indexerDef.getName()));
            }
            
            indexerProcessId = indexerProcessRegistry.registerIndexerProcess(indexerDef.getName(), hostName);
            indexerProcessIds.put(indexerDef.getName(), indexerProcessId);
            
            IndexerConf indexerConf = new XmlIndexerConfReader().read(new ByteArrayInputStream(indexerDef.getConfiguration()));

            IndexSchema indexSchema = loadIndexSchema(indexerDef);
            
            // create and register the indexer
            ResultToSolrMapper mapper = null;
            if (indexerConf.getMapperClass() == null) {
                mapper = new DefaultResultToSolrMapper(indexerDef.getName(),
                        indexerConf.getFieldDefinitions(), indexerConf.getDocumentExtractDefinitions(), indexSchema);
            } else {
                mapper = indexerConf.getMapperClass().newInstance();
                ConfigureUtil.configure(mapper, indexerConf.getGlobalParams());
            }
            
            solr = getSolrServer(indexerDef);
            Indexer indexer = Indexer.createIndexer(indexerDef.getName(), indexerConf, mapper, htablePool, solr);

            int threads = hbaseConf.getInt("hbaseindexer.indexer.threads", 10);
            SepConsumer sepConsumer = new SepConsumer(indexerDef.getSubscriptionId(),
                    indexerDef.getSubscriptionTimestamp(), indexer, threads, hostName,
                    zk, hbaseConf, null);
            
            handle = new IndexerHandle(indexerDef, indexer, sepConsumer, solr);
            handle.start();

            indexers.put(indexerDef.getName(), handle);
            indexerRegistry.register(indexerDef.getName(), indexer);

            log.info("Started indexer for " + indexerDef.getName());
        } catch (Throwable t) {
            if (t instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            log.error("Problem starting indexer " + indexerDef.getName(), t);
            
            try {
                if (indexerProcessId != null) {
                    indexerProcessRegistry.setErrorStatus(indexerProcessId, t);
                }
            } catch (Exception e) {
                log.error("Error setting error status on indexer process " + indexerProcessId, e);
            }

            if (handle != null) {
                // stop any listeners that might have been started
                try {
                    handle.stop();
                } catch (Throwable t2) {
                    if (t2 instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    log.error("Problem stopping consumers for failed-to-start indexer '" +
                            indexerDef.getName() + "'", t2);
                }
            } else {
                // Might be the handle was not yet created, but the solr connection was
                Closer.close(solr);
            }
        }
    }
    
    private IndexSchema loadIndexSchema(IndexerDefinition indexerDef) throws IOException, ParserConfigurationException, SAXException, InterruptedException {
        Map<String, String> connParams = indexerDef.getConnectionParams();
        ZooKeeper zk = new ZooKeeper(connParams.get(SolrConnectionParams.ZOOKEEPER), 30000, HBaseShims.getEmptyWatcherInstance());
        SolrConfigLoader solrConfigLoader = new SolrConfigLoader(connParams.get(SolrConnectionParams.COLLECTION), zk);

        SolrConfig solrConfig = solrConfigLoader.loadSolrConfig();
        SolrResourceLoader loader = solrConfig.getResourceLoader();
        InputSource is = new InputSource(loader.openSchema("schema.xml"));
          is.setSystemId(SystemIdResolver.createSystemIdFromResourceName("schema.xml"));

        IndexSchema indexSchema = new IndexSchema(solrConfig, "schema.xml", is);
        zk.close();
        return indexSchema;

    }

    private void restartIndexer(IndexerDefinition indexerDef) {
        
        IndexerHandle handle = indexers.get(indexerDef.getName());

        if (handle.indexerDef.getOccVersion() >= indexerDef.getOccVersion()) {
            return;
        }

        boolean relevantChanges = !Arrays.equals(handle.indexerDef.getConfiguration(), indexerDef.getConfiguration()) ||
                Objects.equal(handle.indexerDef.getConnectionType(), indexerDef.getConnectionType())
                || !Objects.equal(handle.indexerDef.getConnectionParams(), indexerDef.getConnectionParams());

        if (!relevantChanges) {
            return;
        }

        if (stopIndexer(indexerDef.getName())) {
            startIndexer(indexerDef);
        }
    }

    private boolean stopIndexer(String indexerName) {
        indexerRegistry.unregister(indexerName);

        String processId = indexerProcessIds.remove(indexerName);
        if (processId == null) {
            log.warn("No indexer process to unregister for indexer " + indexerName);
        } else {
            try {
                indexerProcessRegistry.unregisterIndexerProcess(processId);
            } catch (Exception e) {
                log.error("Error unregistering indexer process (from zookeeper): " + indexerProcessIds,  e);
            }
        }

        IndexerHandle handle = indexers.get(indexerName);
        
        if (handle == null) {
            return true;
        }

        try {
            handle.stop();
            indexers.remove(indexerName);
            log.info("Stopped indexer " + indexerName);
            return true;
        } catch (Throwable t) {
            log.fatal("Failed to stop an indexer that should be stopped.", t);
            return false;
        }
    }

    private class MyListener implements IndexerModelListener {
        @Override
        public void process(IndexerModelEvent event) {
            try {
                // Because the actions we take in response to events might take some time, we
                // let the events process by another thread, so that other watchers do not
                // have to wait too long.
                eventQueue.put(event);
            } catch (InterruptedException e) {
                log.info("IndexerSupervisor.IndexerModelListener interrupted.");
            }
        }
    }

    private boolean shouldRunIndexer(IndexerDefinition indexerDef) {
        return indexerDef.getIncrementalIndexingState() == IncrementalIndexingState.SUBSCRIBE_AND_CONSUME &&
                indexerDef.getSubscriptionId() != null &&
                !indexerDef.getLifecycleState().isDeleteState();
    }

    private class IndexerHandle {
        private final IndexerDefinition indexerDef;
        private final Indexer indexer;
        private final SepConsumer sepConsumer;
        private final SolrServer solrServer;

        public IndexerHandle(IndexerDefinition indexerDef, Indexer indexer, SepConsumer sepEventSlave,
                SolrServer solrServer) {
            this.indexerDef = indexerDef;
            this.indexer = indexer;
            this.sepConsumer = sepEventSlave;
            this.solrServer = solrServer;
        }
        
        public void start() throws InterruptedException, KeeperException, IOException {
            sepConsumer.start();
        }

        public void stop() throws InterruptedException {
            Closer.close(sepConsumer);
            Closer.close(solrServer);
            Closer.close(indexer);
        }
    }

    private class EventWorker implements Runnable {
        private volatile boolean stop = false;

        public void stop() {
            stop = true;
        }

        @Override
        public void run() {
            while (!stop) { // We need the stop flag because some code (HBase client code) eats interrupted flags
                if (Thread.interrupted()) {
                    return;
                }

                try {
                    int queueSize = eventQueue.size();
                    if (queueSize >= 10) {
                        log.warn("EventWorker queue getting large, size = " + queueSize);
                    }

                    IndexerModelEvent event = eventQueue.take();
                    if (event.getType() == INDEXER_ADDED || event.getType() == INDEXER_UPDATED) {
                        try {
                            IndexerDefinition indexerDef = indexerModel.getIndexer(event.getIndexerName());
                            if (shouldRunIndexer(indexerDef)) {
                                if (indexers.containsKey(indexerDef.getName())) {
                                    restartIndexer(indexerDef);
                                } else {
                                    startIndexer(indexerDef);
                                }
                            } else {
                                stopIndexer(indexerDef.getName());
                            }
                        } catch (IndexerNotFoundException e) {
                            stopIndexer(event.getIndexerName());
                        } catch (Throwable t) {
                            log.error("Error in IndexerWorker's IndexerModelListener.", t);
                        }
                    } else if (event.getType() == INDEXER_DELETED) {
                        stopIndexer(event.getIndexerName());
                    }
                    eventCount.incrementAndGet();
                } catch (InterruptedException e) {
                    log.info("IndexerWorker.EventWorker interrupted.");
                    return;
                } catch (Throwable t) {
                    log.error("Error processing indexer model event in IndexerWorker.", t);
                }
            }
        }
    }
}
