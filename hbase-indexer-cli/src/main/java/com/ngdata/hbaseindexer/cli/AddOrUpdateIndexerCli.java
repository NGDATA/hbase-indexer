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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.conf.DefaultIndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.conf.IndexerConfException;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.util.IndexerNameValidator;
import com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil;
import joptsimple.ArgumentAcceptingOptionSpec;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.ValueConversionException;
import joptsimple.ValueConverter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.util.Pair;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

/**
 * Base class for the {@link AddIndexerCli} and {@link UpdateIndexerCli}.
 */
public abstract class AddOrUpdateIndexerCli extends BaseIndexCli {
    protected OptionSpec<String> nameOption;
    protected ArgumentAcceptingOptionSpec<String> indexerConfOption;
    protected ArgumentAcceptingOptionSpec<String> indexerComponentFactoryOption;
    protected OptionSpec<Pair<String, String>> connectionParamOption;
    protected OptionSpec<IndexerDefinition.LifecycleState> lifecycleStateOption;
    protected OptionSpec<IndexerDefinition.IncrementalIndexingState> incrementalIdxStateOption;
    protected OptionSpec<IndexerDefinition.BatchIndexingState> batchIdxStateOption;
    protected OptionSpec<String> defaultBatchIndexCliArgumentsOption;
    protected OptionSpec<String> batchIndexCliArgumentsOption;

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        nameOption = parser
                .acceptsAll(Lists.newArrayList("n", "name"), "a name for the index")
                .withRequiredArg().ofType(String.class)
                .required();

        indexerComponentFactoryOption = parser
                .acceptsAll(Lists.newArrayList("r", "indexer-component-factory"), "Indexer component factory class")
                .withRequiredArg().ofType(String.class).describedAs("factoryclass")
                .defaultsTo(DefaultIndexerComponentFactory.class.getName());

        indexerConfOption = parser
                .acceptsAll(Lists.newArrayList("c", "indexer-conf"), "Indexer configuration")
                .withRequiredArg().ofType(String.class).describedAs("indexerconf.xml");

        connectionParamOption = parser
                .acceptsAll(Lists.newArrayList("cp", "connection-param"),
                        "A connection parameter in the form key=value. This option can be specified multiple"
                                + " times. Example: -cp solr.zk=host1,host2 -cp solr.collection=products. In case"
                                + " of update, use an empty value to remove a key: -cp solr.collection=")
                .withRequiredArg()
                .withValuesConvertedBy(new StringPairConverter())
                .describedAs("key=value");

        lifecycleStateOption = parser
                .acceptsAll(Lists.newArrayList("lifecycle"), "Lifecycle state, one of "
                        + LifecycleState.ACTIVE + ", " + LifecycleState.DELETE_REQUESTED)
                .withRequiredArg()
                .withValuesConvertedBy(new EnumConverter<LifecycleState>(LifecycleState.class))
                .defaultsTo(LifecycleState.DEFAULT)
                .describedAs("state");

        incrementalIdxStateOption = parser
                .acceptsAll(Lists.newArrayList("incremental"), "Incremental indexing state, one of "
                        + IncrementalIndexingState.SUBSCRIBE_AND_CONSUME
                        + ", " + IncrementalIndexingState.SUBSCRIBE_DO_NOT_CONSUME
                        + ", " + IncrementalIndexingState.DO_NOT_SUBSCRIBE)
                .withRequiredArg()
                .withValuesConvertedBy(new EnumConverter<IncrementalIndexingState>(IncrementalIndexingState.class))
                .defaultsTo(IncrementalIndexingState.DEFAULT)
                .describedAs("state");

        batchIdxStateOption = parser
                .acceptsAll(Lists.newArrayList("batch"), "Batch indexing state, can only be set to "
                        + BatchIndexingState.BUILD_REQUESTED + (". This will trigger a batch rebuild of the index in "
                        + "\"direct write\" mode (scanning over all records and sending the results to a live solr cluster)."))
                .withRequiredArg()
                .withValuesConvertedBy(new EnumConverter<BatchIndexingState>(BatchIndexingState.class))
                .defaultsTo(BatchIndexingState.DEFAULT)
                .describedAs("state");

        defaultBatchIndexCliArgumentsOption = parser
                .acceptsAll(Lists.newArrayList("dbc", "default-batch-cli-arguments"),
                        "Default batch indexing cli arguments for this indexer. On update, use this option without"
                                + " filename argument to clear the setting. Note that not all options of the map reduce"
                                + " batch index job make sense in this context, because it only supports direct write to"
                                + " a running solr cluster (i.e. --reducers 0)")
                .withOptionalArg().ofType(String.class).describedAs("file-with-arguments");

        batchIndexCliArgumentsOption = parser
                .acceptsAll(Lists.newArrayList("bc", "batch-cli-arguments"),
                        "Batch indexing cli arguments to use for the next batch index build triggered, this overrides"
                                + " the default batch index cli arguments (if any). On update, use this option without"
                                + " filename argument to clear the setting. Note that not all options of the map reduce"
                                + " batch index job make sense in this context, because it only supports direct write to"
                                + " a running solr cluster (i.e. --reducers 0)")
                .withOptionalArg().ofType(String.class).describedAs("file-with-arguments");

        return parser;
    }

    /**
     * Builds an {@link IndexerDefinition} based on the CLI options provided, an optionally starting from
     * an initial state.
     */
    protected IndexerDefinitionBuilder buildIndexerDefinition(OptionSet options, IndexerDefinition oldIndexerDef)
            throws IOException {

        IndexerDefinitionBuilder builder = new IndexerDefinitionBuilder();
        if (oldIndexerDef != null)
            builder.startFrom(oldIndexerDef);

        // name option is always required, so don't need to check for nulliness
        String indexerName = nameOption.value(options);
        IndexerNameValidator.validate(indexerName);
        builder.name(indexerName);

        LifecycleState lifecycleState = lifecycleStateOption.value(options);
        if (lifecycleState != null)
            builder.lifecycleState(lifecycleState);

        IncrementalIndexingState incrementalIdxState = incrementalIdxStateOption.value(options);
        if (incrementalIdxState != null)
            builder.incrementalIndexingState(incrementalIdxState);

        BatchIndexingState batchIdxState = batchIdxStateOption.value(options);
        if (batchIdxState != null)
            builder.batchIndexingState(batchIdxState);

        // connection type is a hardcoded setting
        builder.connectionType("solr");

        Map<String, String> connectionParams = getConnectionParams(options,
                oldIndexerDef != null ? oldIndexerDef.getConnectionParams() : null);
        if (connectionParams != null)
            builder.connectionParams(connectionParams);

        if (oldIndexerDef == null || oldIndexerDef.getIndexerComponentFactory() == null)
            builder.indexerComponentFactory(indexerComponentFactoryOption.value(options));

        byte[] indexerConf = getIndexerConf(options, indexerComponentFactoryOption, indexerConfOption);
        if (indexerConf != null)
            builder.configuration(indexerConf);

        String[] defaultBatchIndexCliArguments = getBatchIndexingCliArguments(options, defaultBatchIndexCliArgumentsOption);
        if (defaultBatchIndexCliArguments != null) {
            if (defaultBatchIndexCliArguments.length == 0) {
                builder.defaultBatchIndexCliArguments(null);
            } else {
                builder.defaultBatchIndexCliArguments(defaultBatchIndexCliArguments);
            }
        }

        String[] batchIndexCliArguments = getBatchIndexingCliArguments(options, batchIndexCliArgumentsOption);
        if (batchIndexCliArguments != null) {
            if (batchIndexCliArguments.length == 0) {
                builder.batchIndexCliArguments(null);
            } else {
                builder.batchIndexCliArguments(batchIndexCliArguments);
            }
        }

        return builder;
    }

    protected byte[] getIndexerConf(OptionSet options, OptionSpec<String> readerOption, OptionSpec<String> configOption)
            throws IOException {
        String componentFactory = readerOption.value(options);

        String fileName = configOption.value(options);
        byte[] data = null;
        if (fileName != null) {
            File file = new File(fileName);
            if (!file.exists()) {
                StringBuilder msg = new StringBuilder();
                msg.append("Specified indexer configuration file not found:\n");
                msg.append(file.getAbsolutePath());
                throw new CliException(msg.toString());
            }

            data = ByteStreams.toByteArray(Files.newInputStreamSupplier(file).getInput());
        }

        try {
            IndexerComponentFactoryUtil.getComponentFactory(componentFactory, new ByteArrayInputStream(data), Maps.<String, String>newHashMap());
        } catch (IndexerConfException e) {
            StringBuilder msg = new StringBuilder();
            msg.append("Failed to parse configuration ").append(fileName).append('\n');
            addExceptionMessages(e, msg);
            throw new CliException(msg.toString());
        }
        return data;
    }

    private void addExceptionMessages(Throwable throwable, StringBuilder builder) {
        Throwable cause = throwable;
        while (cause != null) {
            builder.append(cause.getMessage()).append('\n');
            cause = cause.getCause();
        }
    }

    private Map<String, String> getConnectionParams(OptionSet options, Map<String, String> oldParams) {
        Map<String, String> connectionParams = Maps.newHashMap();
        if (oldParams != null) {
            connectionParams = Maps.newHashMap(oldParams);
        }

        String oldSolrMode = connectionParams.get(SolrConnectionParams.MODE);
        if (oldSolrMode == null) {
            oldSolrMode = "cloud";
        }
        List<String> explicit = Lists.newArrayList();

        for (Pair<String, String> param : connectionParamOption.values(options)) {
            // An empty value indicates a request to remove the key
            if (param.getSecond().length() == 0) {
                connectionParams.remove(param.getFirst());
            } else {
                explicit.add(param.getFirst());
                if (!isValidConnectionParam(param.getFirst())) {
                    System.err.println("WARNING: the following is not a recognized Solr connection parameter: "
                            + param.getFirst());
                }
                connectionParams.put(param.getFirst(), param.getSecond());
            }
        }

        String newSolrMode = connectionParams.get(SolrConnectionParams.MODE);
        if (newSolrMode == null) {
            newSolrMode = "cloud";
        }
        if (oldSolrMode.equals("cloud") && newSolrMode.equals("classic")) {
            // Switch from cloud to classic -- remove any cloud specific parameters
            removeUnlessExplicit(explicit, connectionParams, SolrConnectionParams.COLLECTION);
            removeUnlessExplicit(explicit, connectionParams, SolrConnectionParams.ZOOKEEPER);
        } else if (oldSolrMode.equals("classic") && newSolrMode.equals("cloud")) {
            // Switch from classic to cloud -- remove any cloud specific parameters
            removeUnlessExplicit(explicit, connectionParams, SolrConnectionParams.SHARDER_TYPE);
            removeUnlessExplicit(explicit, connectionParams, SolrConnectionParams.MAX_CONNECTIONS);
            removeUnlessExplicit(explicit, connectionParams, SolrConnectionParams.MAX_CONNECTIONS_PER_HOST);

            // remove any solr.shard.* parameter that wasn't set explicitly
            List<String> shardParams = Lists.newArrayList();
            Pattern pattern = Pattern.compile(Pattern.quote(SolrConnectionParams.SOLR_SHARD_PREFIX) + "\\d+");
            for (String param : connectionParams.keySet()) {
                if (pattern.matcher(param).matches()) {
                    shardParams.add(param);
                }
            }
            for (String shardParam : shardParams) {
                removeUnlessExplicit(explicit, connectionParams, shardParam);
            }
        }

        //TODO
        // if we detect a switch from classic to cloud,
        // automatically clear solr zk param and solr collection param

        // Validate that the minimum required connection params are present
        if (!connectionParams.containsKey(SolrConnectionParams.MODE)
                || connectionParams.get(SolrConnectionParams.MODE).equals("cloud")) {
            // handle cloud params

            if (!connectionParams.containsKey(SolrConnectionParams.ZOOKEEPER)) {
                String solrZk = getZkConnectionString() + "/solr";
                System.err.println("WARNING: no -cp solr.zk specified, will use " + solrZk);
                connectionParams.put("solr.zk", solrZk);
            }

            if (!connectionParams.containsKey(SolrConnectionParams.COLLECTION)) {
                throw new CliException(
                        "ERROR: no -cp solr.collection=collectionName specified (this is required when solr.mode=cloud)");
            }

            // TODO: throw error if sharder type is specified or if shards are listed

        } else if (connectionParams.get(SolrConnectionParams.MODE).equals("classic")) {
            // handle classic params

            // Check that there is at least one shard, and that the shards are valid
            if (SolrConnectionParamUtil.getShards(connectionParams).size() == 0) {
                throw new CliException("ERROR: You need at least one shard when using solr classic");
            }

        } else {
            throw new CliException("ERROR: solr.mode should be 'cloud' or 'classic'. Invalid value: " +
                    connectionParams.get(SolrConnectionParams.MODE));
        }

        return connectionParams;
    }

    /**
     * Removes a connection parameter unless it was set explicitly
     *
     * @param explicit         List of parameters that were set explicitly
     * @param connectionParams Current connectionParams
     * @param param            The parameter to remove
     */
    private void removeUnlessExplicit(List<String> explicit, Map<String, String> connectionParams, String param) {
        if (!explicit.contains(param)) {
            connectionParams.remove(param);
        }
    }

    private boolean isValidConnectionParam(String param) {
        List<String> fixed = Lists.newArrayList(
                SolrConnectionParams.COLLECTION,
                SolrConnectionParams.MODE,
                SolrConnectionParams.SHARDER_TYPE,
                SolrConnectionParams.ZOOKEEPER,
                SolrConnectionParams.MAX_CONNECTIONS,
                SolrConnectionParams.MAX_CONNECTIONS_PER_HOST
        );
        if (fixed.contains(param)) {
            return true;
        }
        if (param.matches(Pattern.quote(SolrConnectionParams.SOLR_SHARD_PREFIX) + "\\d+")) {
            return true;
        }

        return false;
    }

    /**
     * Returns a zero-length array in case the configuration should be removed.
     */
    protected String[] getBatchIndexingCliArguments(OptionSet options, OptionSpec<String> option) throws IOException {
        String fileName = option.value(options);
        if (fileName == null) {
            return new String[0];
        }

        File file = new File(fileName);
        if (!file.exists()) {
            StringBuilder msg = new StringBuilder();
            msg.append("Specified batch cli arguments configuration file not found:\n");
            msg.append(file.getAbsolutePath());
            throw new CliException(msg.toString());
        }

        return Iterables.toArray(Splitter.on(" ").split(FileUtils.readFileToString(file)), String.class);
    }

    /**
     * Converter for jopt-simple that parses key=value pairs.
     */
    private static class StringPairConverter implements ValueConverter<Pair<String, String>> {
        @Override
        public Pair<String, String> convert(String input) {
            int eqPos = input.indexOf('=');
            if (eqPos == -1) {
                throw new ValueConversionException("Parameter should be in the form key=value, which the " +
                        "following is not: '" + input + "'.");
            }
            String key = input.substring(0, eqPos).trim();
            String value = input.substring(eqPos + 1).trim();
            return Pair.newPair(key, value);
        }

        @SuppressWarnings("unchecked")
        @Override
        public Class<Pair<String, String>> valueType() {
            Class<?> pairClass = Pair.class;
            return (Class<Pair<String, String>>) pairClass;
        }

        @Override
        public String valuePattern() {
            return "key=value";
        }
    }

    private static class EnumConverter<T extends Enum<T>> implements ValueConverter<T> {
        Class<T> enumClass;

        EnumConverter(Class<T> enumClass) {
            this.enumClass = enumClass;
        }

        @Override
        public T convert(String input) {
            try {
                return Enum.valueOf(enumClass, input.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new ValueConversionException("Unrecognized value for enum " + enumClass.getSimpleName()
                        + ": '" + input + "'.");
            }
        }

        @Override
        public Class<T> valueType() {
            return enumClass;
        }

        @Override
        public String valuePattern() {
            return null;
        }
    }
}
