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
package com.ngdata.hbaseindexer.cli;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionNameComparator;
import com.ngdata.hbaseindexer.model.api.IndexerModel;
import com.ngdata.hbaseindexer.model.api.IndexerProcess;
import com.ngdata.hbaseindexer.model.api.IndexerProcessRegistry;
import com.ngdata.hbaseindexer.model.impl.IndexerProcessRegistryImpl;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;

/**
 * CLI tool that lists the {@link IndexerDefinition}s defined in the {@link IndexerModel}.
 */
public class ListIndexersCli extends BaseIndexCli {
    public static void main(String[] args) throws Exception {
        new ListIndexersCli().run(args);
    }

    private ListIndexersCli() {
    }

    @Override
    protected String getCmdName() {
        return "list-indexers";
    }

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        parser.accepts("dump", "dump the various config blobs (assumes everything is UTF-8 text)");

        return parser;
    }

    @Override
    public void run(OptionSet options) throws Exception {
        super.run(options);

        List<IndexerDefinition> indexers = new ArrayList<IndexerDefinition>(model.getIndexers());
        Collections.sort(indexers, IndexerDefinitionNameComparator.INSTANCE);

        PrintStream ps = System.out;

        ps.println("Number of indexes: " + indexers.size());
        ps.println();

        for (IndexerDefinition indexer : indexers) {
            ps.println(indexer.getName());
            ps.println("  + Lifecycle state: " + indexer.getLifecycleState());
            ps.println("  + Incremental indexing state: " + indexer.getIncrementalIndexingState());
            ps.println("  + Batch indexing state: " + indexer.getBatchIndexingState());
            ps.println("  + SEP subscription ID: " + indexer.getSubscriptionId());
            ps.println("  + SEP subscription timestamp: " + new DateTime(indexer.getSubscriptionTimestamp()));
            ps.println("  + Connection type: " + indexer.getConnectionType());
            ps.println("  + Connection params:");
            if (indexer.getConnectionParams() == null) {
                ps.println("    + (none)");
            } else {
                for (Map.Entry<String, String> entry : indexer.getConnectionParams().entrySet()) {
                    ps.println("    + " + entry.getKey() + " = " + entry.getValue());
                }
            }
            ps.println("  + Indexer config:");
            printConf(indexer.getConfiguration(), 6, ps, options.has("dump"));
            ps.println("  + Indexer component factory: " + indexer.getIndexerComponentFactory());
            ps.println("  + Additional batch index CLI arguments:");
            printArguments(indexer.getBatchIndexCliArguments(), 6, ps, options.has("dump"));
            ps.println("  + Default additional batch index CLI arguments:");
            printArguments(indexer.getDefaultBatchIndexCliArguments(), 6, ps, options.has("dump"));

            BatchBuildInfo activeBatchBuild = indexer.getActiveBatchBuildInfo();
            if (activeBatchBuild != null) {
                ps.println("  + Active batch build:");
                printBatchBuildInfo(options, ps, activeBatchBuild);
            }

            BatchBuildInfo lastBatchBuild = indexer.getLastBatchBuildInfo();
            if (lastBatchBuild != null) {
                ps.println("  + Last batch build:");
                printBatchBuildInfo(options, ps, lastBatchBuild);
            }
            printProcessStatus(indexer.getName(), ps);
            ps.println();
        }
    }

    private void printProcessStatus(String indexerName, PrintStream printStream) throws InterruptedException, KeeperException {
        int numRunning = 0;
        List<String> failedNodes = Lists.newArrayList();
        IndexerProcessRegistry processRegistry = new IndexerProcessRegistryImpl(zk, conf);
        List<IndexerProcess> indexerProcesses = processRegistry.getIndexerProcesses(indexerName);
        for (IndexerProcess indexerProcess : indexerProcesses) {
            if (indexerProcess.isRunning()) {
                numRunning++;
            } else {
                failedNodes.add(indexerProcess.getHostName());
            }
        }

        printStream.println("  + Processes");
        printStream.printf("    + %d running processes\n", numRunning);
        printStream.printf("    + %d failed processes\n", failedNodes.size());
        for (String failedNode : failedNodes) {
            printStream.printf("      + %s\n", failedNode);
        }

    }

    private void printBatchBuildInfo(OptionSet options, PrintStream ps, BatchBuildInfo batchBuildInfo) throws Exception {
        ps.println("    + Submitted at: " + new DateTime(batchBuildInfo.getSubmitTime()).toString());
        Boolean finishedSuccessful = batchBuildInfo.isFinishedSuccessful();
        ps.println("    + State: " + (finishedSuccessful == null ? "pending..." : (finishedSuccessful ? "SUCCESS" : "FAILED")));
        ps.println("    + Submitted at: " + new DateTime(batchBuildInfo.getSubmitTime()).toString());
        ps.println("    + Hadoop jobs: " + (batchBuildInfo.getMapReduceJobTrackingUrls().isEmpty() ? "(none)" : ""));
        for (Map.Entry<String, String> jobEntry : batchBuildInfo.getMapReduceJobTrackingUrls().entrySet()) {
            ps.println("    + Hadoop Job ID: " + jobEntry.getKey());
            ps.println("    + Tracking URL: " + jobEntry.getValue());
        }
        ps.println("    + Batch build CLI arguments:");
        printArguments(batchBuildInfo.getBatchIndexCliArguments(), 8, ps, options.has("dump"));
    }

    /**
     * Prints out a conf stored in a byte[], under the assumption that it is UTF-8 text.
     */
    private void printConf(byte[] conf, int indent, PrintStream ps, boolean dump) throws Exception {
        String prefix = Strings.repeat(" ", indent);
        if (conf == null) {
            ps.println(prefix + "(none)");
        } else {
            if (dump) {
                String data = new String(conf, "UTF-8");
                for (String line : data.split("\n")) {
                    ps.println(prefix + line);
                }
            } else {
                ps.println(prefix + conf.length + " bytes, use -dump to see content");
            }
        }
    }

    /**
     * Prints out an array of arguments.
     */
    private void printArguments(String[] args, int indent, PrintStream ps, boolean dump) throws Exception {
        String prefix = Strings.repeat(" ", indent);
        if (args == null) {
            ps.println(prefix + "(none)");
        } else {
            if (dump) {
                ps.println(prefix + Joiner.on(" ").join(args));
            } else {
                ps.println(prefix + args.length + " arguments, use -dump to see content");
            }
        }
    }

    private static final String COUNTER_MAP_INPUT_RECORDS = "org.apache.hadoop.mapred.Task$Counter:MAP_INPUT_RECORDS";
    private static final String COUNTER_TOTAL_LAUNCHED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:TOTAL_LAUNCHED_MAPS";
    private static final String COUNTER_NUM_FAILED_MAPS =
            "org.apache.hadoop.mapred.JobInProgress$Counter:NUM_FAILED_MAPS";
    private static final String COUNTER_NUM_FAILED_RECORDS =
            "com.ngdata.hbaseindexer.batchbuild.IndexBatchBuildCounters:NUM_FAILED_RECORDS";
}
