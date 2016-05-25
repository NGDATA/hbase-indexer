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

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.ByteStreams;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.conf.IndexerConf;
import com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.hadoop.PublicAlphaNumericComparator;
import org.apache.solr.hadoop.PublicZooKeeperInspector;
import org.apache.solr.hadoop.SolrCloudPartitioner;
import org.apache.solr.hadoop.SolrInputDocumentWritable;
import org.apache.solr.hadoop.SolrOutputFormat;
import org.apache.solr.hadoop.SolrReducer;
import org.apache.solr.hadoop.TreeMergeMapper;
import org.apache.solr.hadoop.TreeMergeOutputFormat;
import org.apache.solr.hadoop.Utils;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.MalformedURLException;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.ngdata.hbaseindexer.indexer.SolrClientFactory.createHttpSolrClients;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMaxConnectionsPerRoute;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMaxConnectionsTotal;
import static com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil.getSolrMode;
import static org.apache.lucene.util.MathUtil.log;
import static org.apache.solr.hadoop.MapReduceIndexerTool.RESULTS_DIR;

/**
 * Top-level tool for running MapReduce-based indexing pipelines over HBase tables.
 */
public class HBaseMapReduceIndexerTool extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseMapReduceIndexerTool.class);

    private static final String FULL_INPUT_LIST = "full-input-list.txt";

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

        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(indexingSpec.getIndexerComponentFactory(), new ByteArrayInputStream(indexingSpec.getConfiguration()), indexingSpec.getIndexConnectionParams());
        IndexerConf indexerConf = factory.createIndexerConf();

        Map<String, String> params = indexerConf.getGlobalParams();
        String morphlineFile = params.get(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM);
        if (hbaseIndexingOpts.morphlineFile != null) {
            morphlineFile = hbaseIndexingOpts.morphlineFile.getPath();
        }
        if (morphlineFile != null) {
            conf.set(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, new File(morphlineFile).getName());
            addDistributedCacheFile(new File(morphlineFile), conf);
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
            addDistributedCacheFile(hbaseIndexingOpts.log4jConfigFile, conf);
        }

        Job job = Job.getInstance(getConf());
        job.setJobName(getClass().getSimpleName() + "/" + HBaseIndexerMapper.class.getSimpleName());
        job.setJarByClass(HBaseIndexerMapper.class);
//        job.setUserClassesTakesPrecedence(true);

        TableMapReduceUtil.initTableMapperJob(
                hbaseIndexingOpts.getScans(),
                HBaseIndexerMapper.class,
                Text.class,
                SolrInputDocumentWritable.class,
                job);

        // explicitely set hbase configuration on the job because the TableMapReduceUtil overwrites it with the hbase defaults
        // (see HBASE-4297 which is not really fixed in hbase 0.94.6 on all code paths)
        HBaseConfiguration.merge(job.getConfiguration(), getConf());

        int mappers = new JobClient(job.getConfiguration()).getClusterStatus().getMaxMapTasks(); // MR1
        //mappers = job.getCluster().getClusterStatus().getMapSlotCapacity(); // Yarn only
        LOG.info("Cluster reports {} mapper slots", mappers);

        LOG.info("Using these parameters: " +
                        "reducers: {}, shards: {}, fanout: {}, maxSegments: {}",
                new Object[]{hbaseIndexingOpts.reducers, hbaseIndexingOpts.shards, hbaseIndexingOpts.fanout,
                        hbaseIndexingOpts.maxSegments});

        if (hbaseIndexingOpts.isDirectWrite()) {
            CloudSolrClient solrServer = new CloudSolrClient(hbaseIndexingOpts.zkHost);
            int zkSessionTimeout = HBaseIndexerConfiguration.getSessionTimeout(conf);
            solrServer.setZkClientTimeout(zkSessionTimeout);
            solrServer.setZkConnectTimeout(zkSessionTimeout);
            solrServer.setDefaultCollection(hbaseIndexingOpts.collection);

            if (hbaseIndexingOpts.clearIndex) {
                clearSolr(indexingSpec.getIndexConnectionParams());
            }

            // Run a mapper-only MR job that sends index documents directly to a live Solr instance.
            job.setOutputFormatClass(NullOutputFormat.class);
            job.setNumReduceTasks(0);
            job.submit();
            callback.jobStarted(job.getJobID().toString(), job.getTrackingURL());
            if (!waitForCompletion(job, hbaseIndexingOpts.isVerbose)) {
                return -1; // job failed
            }
            commitSolr(indexingSpec.getIndexConnectionParams());
            goodbye(job, programStartTime);
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

            int exitCode = runIndexingPipeline(
                    job, callback, getConf(), hbaseIndexingOpts,
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
        Set<SolrClient> servers = createSolrClients(indexConnectionParams);
        for (SolrClient server : servers) {
            server.deleteByQuery("*:*");
            server.commit(false, false);
            server.shutdown();
        }
    }

    private void commitSolr(Map<String, String> indexConnectionParams) throws SolrServerException, IOException {
        Set<SolrClient> servers = createSolrClients(indexConnectionParams);
        for (SolrClient server : servers) {
            server.commit(false, false);
            server.shutdown();
        }
    }

    private Set<SolrClient> createSolrClients(Map<String, String> indexConnectionParams) throws MalformedURLException {
        String solrMode = getSolrMode(indexConnectionParams);
        if (solrMode.equals("cloud")) {
            String indexZkHost = indexConnectionParams.get(SolrConnectionParams.ZOOKEEPER);
            String collectionName = indexConnectionParams.get(SolrConnectionParams.COLLECTION);
            CloudSolrClient solrServer = new CloudSolrClient(indexZkHost);
            int zkSessionTimeout = HBaseIndexerConfiguration.getSessionTimeout(getConf());
            solrServer.setZkClientTimeout(zkSessionTimeout);
            solrServer.setZkConnectTimeout(zkSessionTimeout);
            solrServer.setDefaultCollection(collectionName);
            return Collections.singleton((SolrClient)solrServer);
        } else if (solrMode.equals("classic")) {
            PoolingClientConnectionManager connectionManager = new PoolingClientConnectionManager();
            connectionManager.setDefaultMaxPerRoute(getSolrMaxConnectionsPerRoute(indexConnectionParams));
            connectionManager.setMaxTotal(getSolrMaxConnectionsTotal(indexConnectionParams));

            HttpClient httpClient = new DefaultHttpClient(connectionManager);
            return new HashSet<SolrClient>(createHttpSolrClients(indexConnectionParams, httpClient));
        } else {
            throw new RuntimeException("Only 'cloud' and 'classic' are valid values for solr.mode, but got " + solrMode);
        }

    }

    static List<List<String>> buildShardUrls(List<Object> urls, Integer numShards) {
        if (urls == null) {
            return null;
        }
        List<List<String>> shardUrls = new ArrayList<List<String>>(urls.size());
        List<String> list = null;

        int sz;
        if (numShards == null) {
            numShards = urls.size();
        }
        sz = (int)Math.ceil(urls.size() / (float)numShards);
        for (int i = 0; i < urls.size(); i++) {
            if (i % sz == 0) {
                list = new ArrayList<String>();
                shardUrls.add(list);
            }
            list.add((String)urls.get(i));
        }

        return shardUrls;
    }

    // do the same as if the user had typed 'hadoop ... --files <file>'
    private void addDistributedCacheFile(File file, Configuration conf) throws IOException {
        String HADOOP_TMP_FILES = "tmpfiles"; // see Hadoop's GenericOptionsParser
        String tmpFiles = conf.get(HADOOP_TMP_FILES, "");
        if (tmpFiles.length() > 0) { // already present?
            tmpFiles = tmpFiles + ",";
        }
        GenericOptionsParser parser = new GenericOptionsParser(
                new Configuration(conf),
                new String[]{"--files", file.getCanonicalPath()});
        String additionalTmpFiles = parser.getConfiguration().get(HADOOP_TMP_FILES);
        assert additionalTmpFiles != null;
        assert additionalTmpFiles.length() > 0;
        tmpFiles += additionalTmpFiles;
        conf.set(HADOOP_TMP_FILES, tmpFiles);

    }

    private boolean waitForCompletion(Job job, boolean isVerbose)
            throws IOException, InterruptedException, ClassNotFoundException {

        LOG.debug("Running job: " + getJobInfo(job));
        boolean success = job.waitForCompletion(isVerbose);
        if (!success) {
            LOG.error("Job failed! " + getJobInfo(job));
        }
        return success;
    }

    static void goodbye(Job job, long startTime) {
        float secs = (System.currentTimeMillis() - startTime) / 1000.0f;
        if (job != null) {
            LOG.info("Succeeded with job: " + getJobInfo(job));
        }
        LOG.info("Success. Done. Program took {} secs. Goodbye.", secs);
    }

    private static String getJobInfo(Job job) {
        return "jobName: " + job.getJobName() + ", jobId: " + job.getJobID();
    }

    int runIndexingPipeline(Job job, JobProcessCallback callback, Configuration conf, HBaseIndexingOptions options,
            long programStartTime, FileSystem fs, Path fullInputList, long numFiles,
            int realMappers, int reducers)
            throws IOException, KeeperException, InterruptedException,
            ClassNotFoundException, FileNotFoundException {
        long startTime;
        float secs;

        Path outputResultsDir = new Path(options.outputDir, RESULTS_DIR);
        Path outputReduceDir = new Path(options.outputDir, "reducers");
        Path outputTreeMergeStep = new Path(options.outputDir, "mtree-merge-output");


        FileOutputFormat.setOutputPath(job, outputReduceDir);


        if (job.getConfiguration().get(JobContext.REDUCE_CLASS_ATTR) == null) { // enable customization
            job.setReducerClass(SolrReducer.class);
        }
        if (options.updateConflictResolver == null) {
            throw new IllegalArgumentException("updateConflictResolver must not be null");
        }
        job.getConfiguration().set(SolrReducer.UPDATE_CONFLICT_RESOLVER, options.updateConflictResolver);
        job.getConfiguration().setInt(SolrOutputFormat.SOLR_RECORD_WRITER_MAX_SEGMENTS, options.maxSegments);

        if (options.zkHost != null) {
            assert options.collection != null;
            /*
             * MapReduce partitioner that partitions the Mapper output such that each
             * SolrInputDocument gets sent to the SolrCloud shard that it would have
             * been sent to if the document were ingested via the standard SolrCloud
             * Near Real Time (NRT) API.
             *
             * In other words, this class implements the same partitioning semantics
             * as the standard SolrCloud NRT API. This enables to mix batch updates
             * from MapReduce ingestion with updates from standard NRT ingestion on
             * the same SolrCloud cluster, using identical unique document keys.
             */
            if (job.getConfiguration().get(JobContext.PARTITIONER_CLASS_ATTR) == null) { // enable customization
                job.setPartitionerClass(SolrCloudPartitioner.class);
            }
            job.getConfiguration().set(SolrCloudPartitioner.ZKHOST, options.zkHost);
            job.getConfiguration().set(SolrCloudPartitioner.COLLECTION, options.collection);
        }
        job.getConfiguration().setInt(SolrCloudPartitioner.SHARDS, options.shards);

        job.setOutputFormatClass(SolrOutputFormat.class);
        if (options.solrHomeDir != null) {
            SolrOutputFormat.setupSolrHomeCache(options.solrHomeDir, job);
        } else {
            assert options.zkHost != null;
            // use the config that this collection uses for the SolrHomeCache.
            SolrZkClient zkClient = PublicZooKeeperInspector.getZkClient(options.zkHost);
            try {
                String configName = PublicZooKeeperInspector.readConfigName(zkClient, options.collection);
                File tmpSolrHomeDir = PublicZooKeeperInspector.downloadConfigDir(zkClient, configName);
                SolrOutputFormat.setupSolrHomeCache(tmpSolrHomeDir, job);
                LOG.debug("Using " + tmpSolrHomeDir + " as solr home");
                options.solrHomeDir = tmpSolrHomeDir;
            } finally {
                zkClient.close();
            }
        }

//    MorphlineMapRunner runner = setupMorphline(job, options);
//    if (options.isDryRun && runner != null) {
//      LOG.info("Indexing {} files in dryrun mode", numFiles);
//      startTime = System.currentTimeMillis();
//      dryRun(job, runner, fs, fullInputList);
//      secs = (System.currentTimeMillis() - startTime) / 1000.0f;
//      LOG.info("Done. Indexing {} files in dryrun mode took {} secs", numFiles, secs);
//      goodbye(null, programStartTime);
//      return 0;
//    }
//    job.getConfiguration().set(MorphlineMapRunner.MORPHLINE_FILE_PARAM, options.morphlineFile.getName());

        job.setNumReduceTasks(reducers);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SolrInputDocumentWritable.class);
        LOG.info("Indexing data into {} reducers", new Object[]{reducers});
        startTime = System.currentTimeMillis();
        job.submit();
        callback.jobStarted(job.getJobID().toString(), job.getTrackingURL());
        if (!waitForCompletion(job, options.isVerbose)) {
            return -1; // job failed
        }

        secs = (System.currentTimeMillis() - startTime) / 1000.0f;
        LOG.info("Done. Indexing data into {} reducers took {} secs", new Object[]{reducers, secs});

        int mtreeMergeIterations = 0;
        if (reducers > options.shards) {
            mtreeMergeIterations = (int)Math.round(log(options.fanout, reducers / options.shards));
        }
        LOG.debug("MTree merge iterations to do: {}", mtreeMergeIterations);
        int mtreeMergeIteration = 1;
        while (reducers > options.shards) { // run a mtree merge iteration
            job = Job.getInstance(conf);
            job.setJarByClass(HBaseMapReduceIndexerTool.class);
            job.setJobName(
                    HBaseMapReduceIndexerTool.class.getName() + "/" + Utils.getShortClassName(HBaseMapReduceIndexerTool.class));
            job.setMapperClass(TreeMergeMapper.class);
            job.setOutputFormatClass(TreeMergeOutputFormat.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            job.setInputFormatClass(NLineInputFormat.class);

            Path inputStepDir = new Path(options.outputDir, "mtree-merge-input-iteration" + mtreeMergeIteration);
            fullInputList = new Path(inputStepDir, FULL_INPUT_LIST);
            LOG.debug("MTree merge iteration {}/{}: Creating input list file for mappers {}",
                    new Object[]{mtreeMergeIteration, mtreeMergeIterations, fullInputList});
            numFiles = createTreeMergeInputDirList(job, outputReduceDir, fs, fullInputList);
            if (numFiles != reducers) {
                throw new IllegalStateException("Not same reducers: " + reducers + ", numFiles: " + numFiles);
            }
            NLineInputFormat.addInputPath(job, fullInputList);
            NLineInputFormat.setNumLinesPerSplit(job, options.fanout);
            FileOutputFormat.setOutputPath(job, outputTreeMergeStep);

            LOG.info("MTree merge iteration {}/{}: Merging {} shards into {} shards using fanout {}", new Object[]{
                    mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout});
            startTime = System.currentTimeMillis();
            job.submit();
            callback.jobStarted(job.getJobID().toString(), job.getTrackingURL());
            if (!waitForCompletion(job, options.isVerbose)) {
                return -1; // job failed
            }
            if (!renameTreeMergeShardDirs(outputTreeMergeStep, job, fs)) {
                return -1;
            }
            secs = (System.currentTimeMillis() - startTime) / 1000.0f;
            LOG.info("MTree merge iteration {}/{}: Done. Merging {} shards into {} shards using fanout {} took {} secs",
                    new Object[]{mtreeMergeIteration, mtreeMergeIterations, reducers, (reducers / options.fanout), options.fanout,
                            secs});

            if (!delete(outputReduceDir, true, fs)) {
                return -1;
            }
            if (!rename(outputTreeMergeStep, outputReduceDir, fs)) {
                return -1;
            }
            assert reducers % options.fanout == 0;
            reducers = reducers / options.fanout;
            mtreeMergeIteration++;
        }
        assert reducers == options.shards;

        // normalize output shard dir prefix, i.e.
        // rename part-r-00000 to part-00000 (stems from zero tree merge iterations)
        // rename part-m-00000 to part-00000 (stems from > 0 tree merge iterations)
        for (FileStatus stats : fs.listStatus(outputReduceDir)) {
            String dirPrefix = SolrOutputFormat.getOutputName(job);
            Path srcPath = stats.getPath();
            if (stats.isDirectory() && srcPath.getName().startsWith(dirPrefix)) {
                String dstName = dirPrefix + srcPath.getName().substring(dirPrefix.length() + "-m".length());
                Path dstPath = new Path(srcPath.getParent(), dstName);
                if (!rename(srcPath, dstPath, fs)) {
                    return -1;
                }
            }
        }
        ;

        // publish results dir
        if (!rename(outputReduceDir, outputResultsDir, fs)) {
            return -1;
        }

        if (options.goLive && !new GoLive().goLive(options, listSortedOutputShardDirs(job, outputResultsDir, fs))) {
            return -1;
        }

        goodbye(job, programStartTime);
        return 0;
    }

    private int createTreeMergeInputDirList(Job job, Path outputReduceDir, FileSystem fs, Path fullInputList)
            throws FileNotFoundException, IOException {

        FileStatus[] dirs = listSortedOutputShardDirs(job, outputReduceDir, fs);
        int numFiles = 0;
        FSDataOutputStream out = fs.create(fullInputList);
        try {
            Writer writer = new BufferedWriter(new OutputStreamWriter(out, "UTF-8"));
            for (FileStatus stat : dirs) {
                LOG.debug("Adding path {}", stat.getPath());
                Path dir = new Path(stat.getPath(), "data/index");
                if (!fs.isDirectory(dir)) {
                    throw new IllegalStateException("Not a directory: " + dir);
                }
                writer.write(dir.toString() + "\n");
                numFiles++;
            }
            writer.close();
        } finally {
            out.close();
        }
        return numFiles;
    }

    private FileStatus[] listSortedOutputShardDirs(Job job, Path outputReduceDir, FileSystem fs) throws FileNotFoundException,
            IOException {

        final String dirPrefix = SolrOutputFormat.getOutputName(job);
        FileStatus[] dirs = fs.listStatus(outputReduceDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(dirPrefix);
            }
        });
        for (FileStatus dir : dirs) {
            if (!dir.isDirectory()) {
                throw new IllegalStateException("Not a directory: " + dir.getPath());
            }
        }

        // use alphanumeric sort (rather than lexicographical sort) to properly handle more than 99999 shards
        Arrays.sort(dirs, new Comparator<FileStatus>() {
            @Override
            public int compare(FileStatus f1, FileStatus f2) {
                return new PublicAlphaNumericComparator().compare(f1.getPath().getName(), f2.getPath().getName());
            }
        });

        return dirs;
    }

    /*
     * You can run MapReduceIndexerTool in Solrcloud mode, and once the MR job completes, you can use
     * the standard solrj Solrcloud API to send doc updates and deletes to SolrCloud, and those updates
     * and deletes will go to the right Solr shards, and it will work just fine.
     *
     * The MapReduce framework doesn't guarantee that input split N goes to the map task with the
     * taskId = N. The job tracker and Yarn schedule and assign tasks, considering data locality
     * aspects, but without regard of the input split# withing the overall list of input splits. In
     * other words, split# != taskId can be true.
     *
     * To deal with this issue, our mapper tasks write a little auxiliary metadata file (per task)
     * that tells the job driver which taskId processed which split#. Once the mapper-only job is
     * completed, the job driver renames the output dirs such that the dir name contains the true solr
     * shard id, based on these auxiliary files.
     *
     * This way each doc gets assigned to the right Solr shard even with #reducers > #solrshards
     *
     * Example for a merge with two shards:
     *
     * part-m-00000 and part-m-00001 goes to outputShardNum = 0 and will end up in merged part-m-00000
     * part-m-00002 and part-m-00003 goes to outputShardNum = 1 and will end up in merged part-m-00001
     * part-m-00004 and part-m-00005 goes to outputShardNum = 2 and will end up in merged part-m-00002
     * ... and so on
     *
     * Also see run() method above where it uses NLineInputFormat.setNumLinesPerSplit(job,
     * options.fanout)
     *
     * Also see TreeMergeOutputFormat.TreeMergeRecordWriter.writeShardNumberFile()
     */
    private boolean renameTreeMergeShardDirs(Path outputTreeMergeStep, Job job, FileSystem fs) throws IOException {
        final String dirPrefix = SolrOutputFormat.getOutputName(job);
        FileStatus[] dirs = fs.listStatus(outputTreeMergeStep, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(dirPrefix);
            }
        });

        for (FileStatus dir : dirs) {
            if (!dir.isDirectory()) {
                throw new IllegalStateException("Not a directory: " + dir.getPath());
            }
        }

        // Example: rename part-m-00004 to _part-m-00004
        for (FileStatus dir : dirs) {
            Path path = dir.getPath();
            Path renamedPath = new Path(path.getParent(), "_" + path.getName());
            if (!rename(path, renamedPath, fs)) {
                return false;
            }
        }

        // Example: rename _part-m-00004 to part-m-00002
        for (FileStatus dir : dirs) {
            Path path = dir.getPath();
            Path renamedPath = new Path(path.getParent(), "_" + path.getName());

            // read auxiliary metadata file (per task) that tells which taskId
            // processed which split# aka solrShard
            Path solrShardNumberFile = new Path(renamedPath, TreeMergeMapper.SOLR_SHARD_NUMBER);
            InputStream in = fs.open(solrShardNumberFile);
            byte[] bytes = ByteStreams.toByteArray(in);
            in.close();
            Preconditions.checkArgument(bytes.length > 0);
            int solrShard = Integer.parseInt(new String(bytes, Charsets.UTF_8));
            if (!delete(solrShardNumberFile, false, fs)) {
                return false;
            }

            // same as FileOutputFormat.NUMBER_FORMAT
            NumberFormat numberFormat = NumberFormat.getInstance();
            numberFormat.setMinimumIntegerDigits(5);
            numberFormat.setGroupingUsed(false);
            Path finalPath = new Path(renamedPath.getParent(), dirPrefix + "-m-" + numberFormat.format(solrShard));

            LOG.info("MTree merge renaming solr shard: " + solrShard + " from dir: " + dir.getPath() + " to dir: " + finalPath);
            if (!rename(renamedPath, finalPath, fs)) {
                return false;
            }
        }
        return true;
    }

    private boolean rename(Path src, Path dst, FileSystem fs) throws IOException {
        boolean success = fs.rename(src, dst);
        if (!success) {
            LOG.error("Cannot rename " + src + " to " + dst);
        }
        return success;
    }

    private boolean delete(Path path, boolean recursive, FileSystem fs) throws IOException {
        boolean success = fs.delete(path, recursive);
        if (!success) {
            LOG.error("Cannot delete " + path);
        }
        return success;
    }

}
