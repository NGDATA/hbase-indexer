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

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.impl.action.HelpArgumentAction;
import net.sourceforge.argparse4j.impl.choice.RangeArgumentChoice;
import net.sourceforge.argparse4j.impl.type.FileArgumentType;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentGroup;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.FeatureControl;
import net.sourceforge.argparse4j.inf.Namespace;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.hadoop.PathArgumentType;
import org.apache.solr.hadoop.dedup.RetainMostRecentUpdateConflictResolver;

/**
 * See http://argparse4j.sourceforge.net and for details see
 * http://argparse4j.sourceforge.net/usage.html
 */
class HBaseIndexerArgumentParser {

    private static final String SHOW_NON_SOLR_CLOUD = "--show-non-solr-cloud";

    private static final Log LOG = LogFactory.getLog(HBaseIndexerArgumentParser.class);

    private boolean showNonSolrCloud = false;
    
    /**
     * Parses the given command line arguments.
     *
     * @return exitCode null indicates the caller shall proceed with processing,
     *         non-null indicates the caller shall exit the program with the
     *         given exit status code.
     */
    public Integer parseArgs(String[] args, Configuration conf, HBaseIndexingOptions opts) {
        assert args != null;
        assert conf != null;
        assert opts != null;

        if (args.length == 0) {
            args = new String[] { "--help" };
        }
        
        showNonSolrCloud = Arrays.asList(args).contains(SHOW_NON_SOLR_CLOUD); // intercept it first
        
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("hadoop [GenericOptions]... jar hbase-indexer-mr-*-job.jar", false)
                .defaultHelp(true)
                .description(
                        "MapReduce batch job driver that takes input data from an HBase table and creates Solr index shards and writes the " +
                        "indexes into HDFS, in a flexible, scalable, and fault-tolerant manner. It also supports merging the output shards " +
                        "into a set of live customer-facing Solr servers in SolrCloud. Optionally, documents can be sent directly from the " + 
                        "mapper tasks to SolrCloud, which is a much less scalable approach but enables updating existing documents in SolrCloud. " +
                        "The program proceeds in one or multiple consecutive MapReduce-based phases, as follows:\n\n" +
                        "1) Mapper phase: This (parallel) phase scans over the input HBase table, extracts the relevant content, and " +
                        "transforms it into SolrInputDocuments. If run as a mapper-only job, this phase also writes the SolrInputDocuments " +
                        "directly to a live SolrCloud cluster. The conversion from HBase records into Solr documents is performed via a " +
                        "hbase-indexer configuration and typically based on a morphline.\n\n" +
                        "2) Reducer phase: This (parallel) phase loads the mapper's SolrInputDocuments into one EmbeddedSolrClient per reducer. " +
                        "Each such reducer and Solr server can be seen as a (micro) shard. The Solr servers store their data in HDFS.\n\n" +
                        "3) Mapper-only merge phase: This (parallel) phase merges the set of reducer shards into the number of " +
                        "Solr shards expected by the user, using a mapper-only job. This phase is omitted if the number of shards is " +
                        "already equal to the number of shards expected by the user\n\n" +
                        "4) Go-live phase: This optional (parallel) phase merges the output shards of the previous phase into a set of " +
                        "live customer-facing Solr servers in SolrCloud. If this phase is omitted you can explicitly point each Solr " +
                        "server to one of the HDFS output shard directories\n\n" +
                        "Fault Tolerance: Mapper and reducer task attempts are retried on failure per the standard MapReduce semantics. " + 
                        "On program startup all data in the --output-dir is deleted if that output directory already exists and " +
                        "--overwrite-output-dir is specified. This means that if the whole job fails you can retry simply by rerunning " +
                        "the program again using the same arguments." 
                        );
                        

        ArgumentGroup hbaseIndexerGroup = parser.addArgumentGroup("HBase Indexer parameters")
                .description("Parameters for specifying the HBase indexer definition and/or where it should be loaded from.");

        Argument indexerZkHostArg = hbaseIndexerGroup.addArgument("--hbase-indexer-zk")
                .metavar("STRING")
                .help("The address of the ZooKeeper ensemble from which to fetch the indexer definition named --hbase-indexer-name. "
                    + "Format is: a list of comma separated host:port pairs, each corresponding to a zk server. "
                    + "Example: '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'");

        Argument indexNameArg = hbaseIndexerGroup.addArgument("--hbase-indexer-name")
                .metavar("STRING")
                .help("The name of the indexer configuration to fetch from the ZooKeeper ensemble specified "
                    + "with --hbase-indexer-zk. Example: myIndexer");

        Argument hbaseIndexerConfigArg = hbaseIndexerGroup.addArgument("--hbase-indexer-file")
                .metavar("FILE")
                .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
                .help("Relative or absolute path to a local HBase indexer XML configuration file. If "
                        + "supplied, this overrides --hbase-indexer-zk and --hbase-indexer-name. "
                        + "Example: /path/to/morphline-hbase-mapper.xml");

        Argument hbaseIndexerComponentFactoryArg = hbaseIndexerGroup.addArgument("--hbase-indexer-component-factory")
                .metavar("STRING")
                .help("Classname of the hbase indexer component factory.");

        ArgumentGroup scanArgumentGroup = parser.addArgumentGroup("HBase scan parameters")
                .description("Parameters for specifying what data is included while reading from HBase.");

        Argument hbaseTableNameArg = scanArgumentGroup.addArgument("--hbase-table-name")
                .metavar("STRING")
                .help("Optional name of the HBase table containing the records to be indexed. If "
                    + "supplied, this overrides the value from the --hbase-indexer-* options. "
                    + "Example: myTable");

        Argument startRowArg = scanArgumentGroup.addArgument("--hbase-start-row")
                .metavar("BINARYSTRING")
                .help("Binary string representation of start row from which to start indexing (inclusive). "
                    + "The format of the supplied row key should use two-digit hex values prefixed by "
                    + "\\x for non-ascii characters (e.g. 'row\\x00'). The semantics of this "
                    + "argument are the same as those for the HBase Scan#setStartRow method. "
                    + "The default is to include the first row of the table. Example: AAAA");

        Argument endRowArg = scanArgumentGroup.addArgument("--hbase-end-row")
                .metavar("BINARYSTRING")
                .help("Binary string representation of end row prefix at which to stop indexing (exclusive). "
                    + "See the description of --hbase-start-row for more information. "
                    + "The default is to include the last row of the table. Example: CCCC");

        Argument startTimeArg = scanArgumentGroup.addArgument("--hbase-start-time")
                .metavar("STRING")
                .help("Earliest timestamp (inclusive) in time range of HBase cells to be included for indexing. "
                    + "The default is to include all cells. Example: 0");

        Argument endTimeArg = scanArgumentGroup.addArgument("--hbase-end-time")
                .metavar("STRING")
                .help("Latest timestamp (exclusive) of HBase cells to be included for indexing. "
                    + "The default is to include all cells. Example: 123456789");
        
        Argument timestampFormatArg = scanArgumentGroup.addArgument("--hbase-timestamp-format")
                .metavar("STRING")
                .help("Timestamp format to be used to interpret --hbase-start-time and --hbase-end-time. " +
                      "This is a java.text.SimpleDateFormat compliant format (see " +
                      "http://docs.oracle.com/javase/6/docs/api/java/text/SimpleDateFormat.html). " +
                      "If this parameter is omitted then the timestamps are interpreted as number of " +
                      "milliseconds since the standard epoch (Unix time). " +
                      "Example: yyyy-MM-dd'T'HH:mm:ss.SSSZ");

        ArgumentGroup solrClusterInfoGroup = parser.addArgumentGroup("Solr cluster arguments")
                .description(
                      "Arguments that provide information about your Solr cluster. "
                    + nonSolrCloud("If you are building shards for a SolrCloud cluster, pass the --zk-host argument. "
                    + "If you are building shards for "
                    + "a Non-SolrCloud cluster, pass the --shard-url argument one or more times. To build indexes for "
                    + "a replicated Non-SolrCloud cluster with --shard-url, pass replica urls consecutively and also pass --shards. "
                    + "Using --go-live requires either --zk-host or --shard-url."));

        Argument zkHostArg = solrClusterInfoGroup.addArgument("--zk-host")
                .metavar("STRING")
                .type(String.class)
                .help("The address of a ZooKeeper ensemble being used by a SolrCloud cluster. "
                    + "This ZooKeeper ensemble will be examined to determine the number of output "
                    + "shards to create as well as the Solr URLs to merge the output shards into when using the --go-live option. "
                    + "Requires that you also pass the --collection to merge the shards into.\n"
                    + "\n"
                    + "The --zk-host option implements the same partitioning semantics as the standard SolrCloud "
                    + "Near-Real-Time (NRT) API. This enables to mix batch updates from MapReduce ingestion with "
                    + "updates from standard Solr NRT ingestion on the same SolrCloud cluster, "
                    + "using identical unique document keys.\n"
                    + "\n"
                    + "Format is: a list of comma separated host:port pairs, each corresponding to a zk "
                    + "server. Example: '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183' If "
                    + "the optional chroot suffix is used the example would look "
                    + "like: '127.0.0.1:2181/solr,127.0.0.1:2182/solr,127.0.0.1:2183/solr' "
                    + "where the client would be rooted at '/solr' and all paths "
                    + "would be relative to this root - i.e. getting/setting/etc... "
                    + "'/foo/bar' would result in operations being run on "
                    + "'/solr/foo/bar' (from the server perspective).\n"
                    + nonSolrCloud("\n"
                    + "If --solr-home-dir is not specified, the Solr home directory for the collection "
                    + "will be downloaded from this ZooKeeper ensemble."));

        Argument shardUrlsArg = nonSolrCloud(solrClusterInfoGroup.addArgument("--shard-url")
                .metavar("URL")
                .type(String.class)
                .action(Arguments.append())
                .help("Solr URL to merge resulting shard into if using --go-live. "
                    + "Example: http://solr001.mycompany.com:8983/solr/collection1. "
                    + "Multiple --shard-url arguments can be specified, one for each desired shard. "
                    + "If you are merging shards into a SolrCloud cluster, use --zk-host instead."));
        
        Argument shardsArg = nonSolrCloud(solrClusterInfoGroup.addArgument("--shards")
                .metavar("INTEGER")
                .type(Integer.class).choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
                .help("Number of output shards to generate."));

        ArgumentGroup goLiveGroup = parser.addArgumentGroup("Go live arguments")
                .description("Arguments for merging the shards that are built into a live Solr cluster. "
                          + "Also see the Cluster arguments.");

        Argument goLiveArg = goLiveGroup.addArgument("--go-live")
                .action(Arguments.storeTrue())
                .help("Allows you to optionally merge the final index shards into a live Solr cluster after they are built. "
                    + "You can pass the ZooKeeper address with --zk-host and the relevant cluster information will be auto detected. "
                    + nonSolrCloud("If you are not using a SolrCloud cluster, --shard-url arguments can be used to specify each SolrCore to merge "
                    + "each shard into."));

        Argument collectionArg = goLiveGroup.addArgument("--collection")
                .metavar("STRING")
                .help("The SolrCloud collection to merge shards into when using --go-live and --zk-host. Example: collection1");

        Argument goLiveThreadsArg = goLiveGroup.addArgument("--go-live-threads")
                .metavar("INTEGER")
                .type(Integer.class)
                .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
                .setDefault(1000)
                .help("Tuning knob that indicates the maximum number of live merges to run in parallel at one time.");

        ArgumentGroup optionalGroup = parser.addArgumentGroup("Optional arguments");

        optionalGroup.addArgument("--help", "-help", "-h").help("Show this help message and exit")
                .action(new HelpArgumentAction() {
                    @Override
                    public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value) throws ArgumentParserException {
                      parser.printHelp(new PrintWriter(System.out, true));
                      System.out.println();
                      System.out.print(ToolRunnerHelpFormatter.getGenericCommandUsage());
                      System.out.println("Examples: \n\n" +
                        "# (Re)index a table in GoLive mode based on a local indexer config file\n" +
                        "hadoop --config /etc/hadoop/conf \\\n" +
                        "  jar hbase-indexer-mr-*-job.jar \\\n" +
                        "  --conf /etc/hbase/conf/hbase-site.xml \\\n" +
                        "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
                        "  --hbase-indexer-file indexer.xml \\\n" +
                        "  --zk-host 127.0.0.1/solr \\\n" +
                        "  --collection collection1 \\\n" +
                        "  --go-live \\\n" +
                        "  --log4j src/test/resources/log4j.properties\n\n" + 
                        
                        "# (Re)index a table in GoLive mode using a local morphline-based indexer config file\n" +
                        "# Also include extra library jar file containing JSON tweet Java parser:\n" +
                        "hadoop --config /etc/hadoop/conf \\\n" +
                        "  jar hbase-indexer-mr-*-job.jar \\\n" +
                        "  --conf /etc/hbase/conf/hbase-site.xml \\\n" +
                        "  --libjars /path/to/kite-morphlines-twitter-0.10.0.jar \\\n" + 
                        "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
                        "  --hbase-indexer-file src/test/resources/morphline_indexer_without_zk.xml \\\n" +
                        "  --zk-host 127.0.0.1/solr \\\n" +
                        "  --collection collection1 \\\n" +
                        "  --go-live \\\n" +
                        "  --morphline-file src/test/resources/morphlines.conf \\\n" +
                        "  --output-dir hdfs://c2202.mycompany.com/user/$USER/test \\\n" + 
                        "  --overwrite-output-dir \\\n" + 
                        "  --log4j src/test/resources/log4j.properties\n\n" +
                        
                        "# (Re)index a table in GoLive mode\n" +
                        "hadoop --config /etc/hadoop/conf \\\n" +
                        "  jar hbase-indexer-mr-*-job.jar \\\n" +
                        "  --conf /etc/hbase/conf/hbase-site.xml \\\n" +
                        "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
                        "  --hbase-indexer-file indexer.xml \\\n" +
                        "  --zk-host 127.0.0.1/solr \\\n" +
                        "  --collection collection1 \\\n" +
                        "  --go-live \\\n" +
                        "  --log4j src/test/resources/log4j.properties\n\n" +
                        
                        "# (Re)index a table with direct writes to SolrCloud\n" +
                        "hadoop --config /etc/hadoop/conf \\\n" +
                        "  jar hbase-indexer-mr-*-job.jar \\\n" +
                        "  --conf /etc/hbase/conf/hbase-site.xml \\\n" +
                        "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
                        "  --hbase-indexer-file indexer.xml \\\n" +
                        "  --zk-host 127.0.0.1/solr \\\n" +
                        "  --collection collection1 \\\n" +
                        "  --reducers 0 \\\n" +
                        "  --log4j src/test/resources/log4j.properties\n\n" +
                        
                        "# (Re)index a table based on a indexer config stored in ZK\n" +
                        "hadoop --config /etc/hadoop/conf \\\n" +
                        "  jar hbase-indexer-mr-*-job.jar \\\n" +
                        "  --conf /etc/hbase/conf/hbase-site.xml \\\n" +
                        "  -D 'mapred.child.java.opts=-Xmx500m' \\\n" + 
                        "  --hbase-indexer-zk zk01 \\\n" +
                        "  --hbase-indexer-name docindexer \\\n" +
                        "  --go-live \\\n" +
                        "  --log4j src/test/resources/log4j.properties\n\n"); 

                      throw new FoundHelpArgument(); // Trick to prevent processing of any remaining arguments
                    }
                  });

        Argument outputDirArg = optionalGroup.addArgument("--output-dir")
                .metavar("HDFS_URI")
                .type(new PathArgumentType(conf) {
                    @Override
                    public Path convert(ArgumentParser parser, Argument arg, String value)
                            throws ArgumentParserException {
                        Path path = super.convert(parser, arg, value);
                        if ("hdfs".equals(path.toUri().getScheme())
                                && path.toUri().getAuthority() == null) {
                            // TODO: consider defaulting to hadoop's
                            // fs.default.name here or in
                            // SolrRecordWriter.createEmbeddedSolrClient()
                            throw new ArgumentParserException("Missing authority in path URI: "
                                    + path, parser);
                        }
                        return path;
                    }
                }.verifyHasScheme().verifyIsAbsolute().verifyCanWriteParent())
                .help("HDFS directory to write Solr indexes to. Inside there one output directory per shard will be generated. "
                    + "Example: hdfs://c2202.mycompany.com/user/$USER/test");
        
        Argument overwriteOutputDirArg = optionalGroup.addArgument("--overwrite-output-dir")
                .action(Arguments.storeTrue())
                .help("Overwrite the directory specified by --output-dir if it already exists. Using this parameter will result in " +
                      "the output directory being recursively deleted at job startup.");

        Argument morphlineFileArg = optionalGroup.addArgument("--morphline-file")
                .metavar("FILE")
                .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
                .help("Relative or absolute path to a local config file that contains one or more morphlines. " +
                      "The file must be UTF-8 encoded. The file will be uploaded to each MR task. " +
                      "If supplied, this overrides the value from the --hbase-indexer-* options. " +
                      "Example: /path/to/morphlines.conf");
              
        Argument morphlineIdArg = optionalGroup.addArgument("--morphline-id")
                .metavar("STRING")
                .type(String.class)
                .help("The identifier of the morphline that shall be executed within the morphline config file, " +
                      "e.g. specified by --morphline-file. If the --morphline-id option is ommitted the first (i.e. " +
                      "top-most) morphline within the config file is used. If supplied, this overrides the value " +
                      "from the --hbase-indexer-* options. Example: morphline1 ");
                
        Argument solrHomeDirArg = nonSolrCloud(optionalGroup.addArgument("--solr-home-dir")
                .metavar("DIR")
                .type(new FileArgumentType() {
                    @Override
                    public File convert(ArgumentParser parser, Argument arg, String value)
                            throws ArgumentParserException {
                        File solrHomeDir = super.convert(parser, arg, value);
                        File solrConfigFile = new File(new File(solrHomeDir, "conf"),
                                "solrconfig.xml");
                        new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead()
                                .convert(parser, arg, solrConfigFile.getPath());
                        return solrHomeDir;
                    }
                }.verifyIsDirectory().verifyCanRead())
                .required(false)
                .help("Relative or absolute path to a local dir containing Solr conf/ dir and in particular "
                    + "conf/solrconfig.xml and optionally also lib/ dir. This directory will be uploaded to each MR task. "
                    + "Example: src/test/resources/solr/minimr"));

        Argument updateConflictResolverArg = optionalGroup.addArgument("--update-conflict-resolver")
                .metavar("FQCN")
                .type(String.class)
                .setDefault(RetainMostRecentUpdateConflictResolver.class.getName())
                .help("Fully qualified class name of a Java class that implements the UpdateConflictResolver interface. "
                    + "This enables deduplication and ordering of a series of document updates for the same unique document "
                    + "key. For example, a MapReduce batch job might index multiple files in the same job where some of the "
                    + "files contain old and new versions of the very same document, using the same unique document key.\n"
                    + "Typically, implementations of this interface forbid collisions by throwing an exception, or ignore all but "
                    + "the most recent document version, or, in the general case, order colliding updates ascending from least "
                    + "recent to most recent (partial) update. The caller of this interface (i.e. the Hadoop Reducer) will then "
                    + "apply the updates to Solr in the order returned by the orderUpdates() method.\n"
                    + "The default RetainMostRecentUpdateConflictResolver implementation ignores all but the most recent document "
                    + "version, based on a configurable numeric Solr field, which defaults to the file_last_modified timestamp");

        Argument reducersArg = optionalGroup.addArgument("--reducers")
                .metavar("INTEGER")
                .type(Integer.class)
                .choices(new RangeArgumentChoice(-2, Integer.MAX_VALUE))
                // TODO: also support X% syntax where X is an integer
                .setDefault(-1)
                .help("Tuning knob that indicates the number of reducers to index into. "
                    + "0 indicates that no reducers should be used, and documents should be sent directly from the mapper tasks to live Solr servers. "
                    + "-1 indicates use all reduce slots available on the cluster. "
                    + "-2 indicates use one reducer per output shard, which disables the mtree merge MR algorithm. "
                    + "The mtree merge MR algorithm improves scalability by spreading load "
                    + "(in particular CPU load) among a number of parallel reducers that can be much larger than the number "
                    + "of solr shards expected by the user. It can be seen as an extension of concurrent lucene merges "
                    + "and tiered lucene merges to the clustered case. The subsequent mapper-only phase "
                    + "merges the output of said large number of reducers to the number of shards expected by the user, "
                    + "again by utilizing more available parallelism on the cluster.");

        Argument fanoutArg = optionalGroup.addArgument("--fanout")
                .metavar("INTEGER")
                .type(Integer.class)
                .choices(new RangeArgumentChoice(2, Integer.MAX_VALUE))
                .setDefault(Integer.MAX_VALUE)
                .help(FeatureControl.SUPPRESS);

        Argument maxSegmentsArg = optionalGroup.addArgument("--max-segments")
                .metavar("INTEGER")
                .type(Integer.class)
                .choices(new RangeArgumentChoice(1, Integer.MAX_VALUE))
                .setDefault(1)
                .help("Tuning knob that indicates the maximum number of segments to be contained on output in the index of "
                    + "each reducer shard. After a reducer has built its output index it applies a merge policy to merge segments "
                    + "until there are <= maxSegments lucene segments left in this index. "
                    + "Merging segments involves reading and rewriting all data in all these segment files, "
                    + "potentially multiple times, which is very I/O intensive and time consuming. "
                    + "However, an index with fewer segments can later be merged faster, "
                    + "and it can later be queried faster once deployed to a live Solr serving shard. "
                    + "Set maxSegments to 1 to optimize the index for low query latency. "
                    + "In a nutshell, a small maxSegments value trades indexing latency for subsequently improved query latency. "
                    + "This can be a reasonable trade-off for batch indexing systems.");

        Argument fairSchedulerPoolArg = optionalGroup.addArgument("--fair-scheduler-pool")
                .metavar("STRING")
                .help("Optional tuning knob that indicates the name of the fair scheduler pool to submit jobs to. "
                    + "The Fair Scheduler is a pluggable MapReduce scheduler that provides a way to share large clusters. "
                    + "Fair scheduling is a method of assigning resources to jobs such that all jobs get, on average, an "
                    + "equal share of resources over time. When there is a single job running, that job uses the entire "
                    + "cluster. When other jobs are submitted, tasks slots that free up are assigned to the new jobs, so "
                    + "that each job gets roughly the same amount of CPU time. Unlike the default Hadoop scheduler, which "
                    + "forms a queue of jobs, this lets short jobs finish in reasonable time while not starving long jobs. "
                    + "It is also an easy way to share a cluster between multiple of users. Fair sharing can also work with "
                    + "job priorities - the priorities are used as weights to determine the fraction of total compute time "
                    + "that each job gets.");

        Argument dryRunArg = optionalGroup.addArgument("--dry-run")
                .action(Arguments.storeTrue())
                .help("Run in local mode and print documents to stdout instead of loading them into Solr. This executes "
                    + "the morphline in the client process (without submitting a job to MR) for quicker turnaround during "
                    + "early trial & debug sessions.");

        Argument log4jConfigFileArg = optionalGroup.addArgument("--log4j")
                .metavar("FILE")
                .type(new FileArgumentType().verifyExists().verifyIsFile().verifyCanRead())
                .help("Relative or absolute path to a log4j.properties config file on the local file system. This file "
                    + "will be uploaded to each MR task. Example: /path/to/log4j.properties");

        Argument verboseArg = optionalGroup.addArgument("--verbose", "-v")
                .action(Arguments.storeTrue())
                .help("Turn on verbose output.");

        Argument clearIndexArg = optionalGroup.addArgument("--clear-index")
                .action(Arguments.storeTrue())
                .help("Will attempt to delete all entries in a solr index before starting batch build. This is not " +
                        "transactional so if the build fails the index will be empty.");
        
        optionalGroup.addArgument(SHOW_NON_SOLR_CLOUD)
                .action(Arguments.storeTrue())
                .help("Also show options for Non-SolrCloud mode as part of --help.");
    
        Namespace ns;
        try {
            ns = parser.parseArgs(args);
        } catch (FoundHelpArgument e) {
            return 0;
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return 1;
        }

        opts.log4jConfigFile = (File) ns.get(log4jConfigFileArg.getDest());
        if (opts.log4jConfigFile != null) {
            PropertyConfigurator.configure(opts.log4jConfigFile.getPath());
        }
        LOG.debug("Parsed command line args: " + ns);

        opts.outputDir = (Path) ns.get(outputDirArg.getDest());
        opts.overwriteOutputDir = ns.getBoolean(overwriteOutputDirArg.getDest());
        opts.reducers = ns.getInt(reducersArg.getDest());
        opts.updateConflictResolver = ns.getString(updateConflictResolverArg.getDest());
        opts.fanout = ns.getInt(fanoutArg.getDest());
        opts.maxSegments = ns.getInt(maxSegmentsArg.getDest());
        opts.morphlineFile = (File) ns.get(morphlineFileArg.getDest());
        opts.morphlineId = ns.getString(morphlineIdArg.getDest());
        opts.solrHomeDir = (File) ns.get(solrHomeDirArg.getDest());
        opts.fairSchedulerPool = ns.getString(fairSchedulerPoolArg.getDest());
        opts.isDryRun = ns.getBoolean(dryRunArg.getDest());
        opts.isVerbose = ns.getBoolean(verboseArg.getDest());
        opts.zkHost = ns.getString(zkHostArg.getDest());
        opts.shards = ns.getInt(shardsArg.getDest());
        opts.shardUrls = HBaseMapReduceIndexerTool.buildShardUrls(ns.getList(shardUrlsArg.getDest()), opts.shards);
        opts.goLive = ns.getBoolean(goLiveArg.getDest());
        opts.goLiveThreads = ns.getInt(goLiveThreadsArg.getDest());
        opts.collection = ns.getString(collectionArg.getDest());
        opts.clearIndex = ns.getBoolean(clearIndexArg.getDest());

        opts.hbaseIndexerComponentFactory = (String) ns.get(hbaseIndexerComponentFactoryArg.getDest());
        opts.hbaseIndexerConfigFile = (File) ns.get(hbaseIndexerConfigArg.getDest());
        opts.hbaseIndexerZkHost = ns.getString(indexerZkHostArg.getDest());
        opts.hbaseIndexerName = ns.getString(indexNameArg.getDest());
        opts.hbaseTableName = ns.getString(hbaseTableNameArg.getDest());
        opts.hbaseStartRow = ns.getString(startRowArg.getDest());
        opts.hbaseEndRow = ns.getString(endRowArg.getDest());
        opts.hbaseStartTimeString = ns.getString(startTimeArg.getDest());
        opts.hbaseEndTimeString = ns.getString(endTimeArg.getDest());
        opts.hbaseTimestampFormat = ns.getString(timestampFormatArg.getDest());

        try {
            try {
                opts.evaluate();
            } catch (IllegalStateException ise) {
                throw new ArgumentParserException(ise.getMessage(), parser);
            }
        } catch (ArgumentParserException e) {
            parser.handleError(e);
            return 1;
        }

        return null;
    }
    
    // make it a "hidden" option, i.e. the option is functional and enabled but not shown in --help output
    private Argument nonSolrCloud(Argument arg) {
        return showNonSolrCloud ? arg : arg.help(FeatureControl.SUPPRESS); 
    }

    private String nonSolrCloud(String msg) {
        return showNonSolrCloud ? msg : "";
    }
  
    /**
     * Marker trick to prevent processing of any remaining arguments once --help
     * option has been parsed
     */
    private static final class FoundHelpArgument extends RuntimeException {
    }

}
