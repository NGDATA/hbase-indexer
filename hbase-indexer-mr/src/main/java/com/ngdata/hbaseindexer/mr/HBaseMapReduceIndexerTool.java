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
package com.ngdata.hbaseindexer.mr;

import static com.ngdata.hbaseindexer.indexer.SolrServerFactory.createHttpSolrServers;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMaxConnectionsPerRoute;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMaxConnectionsTotal;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMode;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.ngdata.hbaseindexer.SolrConnectionParams;

import com.google.common.base.Charsets;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.hadoop.ForkedMapReduceIndexerTool;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Top-level tool for running MapReduce-based indexing pipelines over HBase tables.
 */
public class HBaseMapReduceIndexerTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(ForkedMapReduceIndexerTool.class);

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Configuration(), new HBaseMapReduceIndexerTool(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        return run(args, new NopJobProcessCallback());
    }

    public int run(String[] args, JobProcessCallback callback) throws Exception {
        HBaseIndexingOptions hbaseIndexingOpts = new HBaseIndexingOptions(getConf());
        Integer exitCode = new HBaseIndexerArgumentParser().parseArgs(args, getConf(), hbaseIndexingOpts);
        if (exitCode != null) {
            return exitCode;
        }

        return run(hbaseIndexingOpts, callback);
    }

    public int run(HBaseIndexingOptions hbaseIndexingOpts, JobProcessCallback callback) throws Exception {

        if (hbaseIndexingOpts.isDryRun) {
            return new IndexerDryRun(hbaseIndexingOpts, getConf(), System.out).run();
        }

        long programStartTime = System.currentTimeMillis();
        Configuration conf = getConf();

        IndexingSpecification indexingSpec = hbaseIndexingOpts.getIndexingSpecification();

        conf.set(HBaseIndexerMapper.INDEX_COMPONENT_FACTORY_KEY, indexingSpec.getIndexerComponentFactory());
        conf.set(HBaseIndexerMapper.INDEX_CONFIGURATION_CONF_KEY, new String(indexingSpec.getConfiguration(), Charsets.UTF_8));
        conf.set(HBaseIndexerMapper.INDEX_NAME_CONF_KEY, indexingSpec.getIndexerName());
        conf.set(HBaseIndexerMapper.TABLE_NAME_CONF_KEY, indexingSpec.getTableName());
        HBaseIndexerMapper.configureIndexConnectionParams(conf, indexingSpec.getIndexConnectionParams());

        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(indexingSpec.getIndexerComponentFactory(), new ByteArrayInputStream(indexingSpec.getConfiguration()), Maps.<String, String>newHashMap());
        IndexerConf indexerConf = factory.createIndexerConf();

        Map<String, String> params = indexerConf.getGlobalParams();
        String morphlineFile = params.get(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM);
        if (hbaseIndexingOpts.morphlineFile != null) {
            morphlineFile = hbaseIndexingOpts.morphlineFile.getPath();
        }
        if (morphlineFile != null) {
            conf.set(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, new File(morphlineFile).getName());
            ForkedMapReduceIndexerTool.addDistributedCacheFile(new File(morphlineFile), conf);
        }

        String morphlineId = params.get(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM);
        if (hbaseIndexingOpts.morphlineId != null) {
            morphlineId = hbaseIndexingOpts.morphlineId;
        }
        if (morphlineId != null) {
            conf.set(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM, morphlineId);
        }

        conf.setBoolean(HBaseIndexerMapper.INDEX_DIRECT_WRITE_CONF_KEY, hbaseIndexingOpts.isDirectWrite());

        if (hbaseIndexingOpts.fairSchedulerPool != null) {
            conf.set("mapred.fairscheduler.pool", hbaseIndexingOpts.fairSchedulerPool);
        }

        // switch off a false warning about allegedly not implementing Tool
        // also see http://hadoop.6.n7.nabble.com/GenericOptionsParser-warning-td8103.html
        // also see https://issues.apache.org/jira/browse/HADOOP-8183
        getConf().setBoolean("mapred.used.genericoptionsparser", true);

        if (hbaseIndexingOpts.log4jConfigFile != null) {
            Utils.setLogConfigFile(hbaseIndexingOpts.log4jConfigFile, getConf());
            ForkedMapReduceIndexerTool.addDistributedCacheFile(hbaseIndexingOpts.log4jConfigFile, conf);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName(getClass().getSimpleName() + "/" + HBaseIndexerMapper.class.getSimpleName());
        job.setJarByClass(HBaseIndexerMapper.class);
        job.setUserClassesTakesPrecedence(true);

        TableMapReduceUtil.initTableMapperJob(
                hbaseIndexingOpts.getScans(),
                HBaseIndexerMapper.class,
                Text.class,
                SolrInputDocumentWritable.class,
                job);

        int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
        //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
        LOG.info("Cluster reports {} mapper slots", mappers);

        LOG.info("Using these parameters: " +
                "reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
                new Object[]{hbaseIndexingOpts.reducers, hbaseIndexingOpts.shards, hbaseIndexingOpts.fanout,
                        hbaseIndexingOpts.maxSegments});

        if (hbaseIndexingOpts.isDirectWrite()) {
            CloudSolrServer solrServer = new CloudSolrServer(hbaseIndexingOpts.zkHost);
            solrServer.setDefaultCollection(hbaseIndexingOpts.collection);

            if (hbaseIndexingOpts.clearIndex) {
                clearSolr(indexingSpec.getIndexConnectionParams());
            }

            // Run a mapper-only MR job that sends index documents directly to a live Solr instance.
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
            job.submit();
            callback.jobStarted(job.getJobID().toString(), job.getTrackingURL());
            if (!ForkedMapReduceIndexerTool.waitForCompletion(job, hbaseIndexingOpts.isVerbose)) {
                return -1; // job failed
            }
            commitSolr(indexingSpec.getIndexConnectionParams());
            ForkedMapReduceIndexerTool.goodbye(job, programStartTime);
            return 0;
        } else {
            FileSystem fileSystem = FileSystem.get(getConf());

            if (fileSystem.exists(hbaseIndexingOpts.outputDir)) {
                if (hbaseIndexingOpts.overwriteOutputDir) {
                    LOG.info("Removing existing output directory {}", hbaseIndexingOpts.outputDir);
                    if (!fileSystem.delete(hbaseIndexingOpts.outputDir, true)) {
                        LOG.error("Deleting output directory '{}' failed", hbaseIndexingOpts.outputDir);
                        return -1;
                    }
                } else {
                    LOG.error("Output directory '{}' already exists. Run with --overwrite-output-dir to " +
                            "overwrite it, or remove it manually", hbaseIndexingOpts.outputDir);
                    return -1;
                }
            }

            int exitCode = ForkedMapReduceIndexerTool.runIndexingPipeline(
                    job, callback, getConf(), hbaseIndexingOpts.asOptions(),
                    programStartTime,
                    fileSystem,
                    null, -1, // File-based parameters
                    -1, // num mappers, only of importance for file-based indexing
                    hbaseIndexingOpts.reducers
            );


            if (hbaseIndexingOpts.isGeneratedOutputDir()) {
                LOG.info("Deleting generated output directory " + hbaseIndexingOpts.outputDir);
                fileSystem.delete(hbaseIndexingOpts.outputDir, true);
            }
            return exitCode;
        }
    }

    private void clearSolr(Map<String, String> indexConnectionParams) throws SolrServerException, IOException {
        Set<SolrServer> servers = createSolrServers(indexConnectionParams);
        for (SolrServer server : servers) {
            server.deleteByQuery("*:*");
            server.commit(false, false);
            server.shutdown();
        }
    }

    private void commitSolr(Map<String, String> indexConnectionParams) throws SolrServerException, IOException {
        Set<SolrServer> servers = createSolrServers(indexConnectionParams);
        for (SolrServer server : servers) {
            server.commit(false, false);
            server.shutdown();
        }
    }

    private Set<SolrServer> createSolrServers(Map<String, String> indexConnectionParams) throws MalformedURLException {
        String solrMode = getSolrMode(indexConnectionParams);
        if (solrMode.equals("cloud")) {
            String indexZkHost = indexConnectionParams.get(SolrConnectionParams.ZOOKEEPER);
            String collectionName = indexConnectionParams.get(SolrConnectionParams.COLLECTION);
            CloudSolrServer solrServer = new CloudSolrServer(indexZkHost);
            solrServer.setDefaultCollection(collectionName);
            return Collections.singleton((SolrServer) solrServer);
        } else if (solrMode.equals("classic")) {
            PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
            connectionManager.setDefaultMaxPerRoute(getSolrMaxConnectionsPerRoute(indexConnectionParams));
            connectionManager.setMaxTotal(getSolrMaxConnectionsTotal(indexConnectionParams));

            HttpClient httpClient = new DefaultHttpClient(connectionManager);
            return new HashSet<SolrServer>(createHttpSolrServers(indexConnectionParams, httpClient));
        } else {
            throw new RuntimeException("Only 'cloud' and 'classic' are valid values for solr.mode, but got " + solrMode);
        }

    }
}
