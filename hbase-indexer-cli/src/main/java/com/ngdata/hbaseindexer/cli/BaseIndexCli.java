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

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.ConfKeys;
import com.ngdata.hbaseindexer.HBaseIndexerConfiguration;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerModelImpl;
import com.ngdata.hbaseindexer.util.zookeeper.StateWatchingZooKeeper;
import com.ngdata.sep.util.io.Closer;
import com.ngdata.sep.util.zookeeper.ZooKeeperItf;
import com.ngdata.sep.util.zookeeper.ZooKeeperOperation;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public abstract class BaseIndexCli extends BaseCli {
    private OptionSpec<String> zkOption;
    private String zkConnectionString;
    protected Configuration conf;
    protected ZooKeeperItf zk;
    protected WriteableIndexerModel model;

    private static final String ZK_ENV_VAR = "HBASE_INDEXER_CLI_ZK";
    private static final String DEFAULT_ZK = "localhost:2181";

    @Override
    protected OptionParser setupOptionParser() {
        OptionParser parser = super.setupOptionParser();

        zkOption = parser
                        .acceptsAll(Lists.newArrayList("z", "zookeeper"), "ZooKeeper connection string. Can also be " +
                                "specified through environment variable " + ZK_ENV_VAR)
                        .withRequiredArg().ofType(String.class).describedAs("connection-string");

        return parser;
    }

    @Override
    protected void run(OptionSet options) throws Exception {
        conf = HBaseIndexerConfiguration.create();

        if (!options.has(zkOption)) {
            String message;
            zkConnectionString = System.getProperty(ZK_ENV_VAR);
            if (zkConnectionString != null) {
                message = "Using ZooKeeper connection string specified in " + ZK_ENV_VAR + ": " + zkConnectionString;
            } else {
                zkConnectionString = DEFAULT_ZK;
                message = "ZooKeeper connection string not specified, using default: " + DEFAULT_ZK;
            }

            // to stderr: makes that sample config dumps of e.g. tester tool do not start with this line, and
            // can thus be redirected to a file without further editing.
            System.err.println(message);
            System.err.println();
        } else {
            zkConnectionString = zkOption.value(options);
        }

        connectWithZooKeeper();

        model = new IndexerModelImpl(zk, conf.get(ConfKeys.ZK_ROOT_NODE));
    }

    @Override
    protected void cleanup() {
        Closer.close(model);
        Closer.close(zk);
        super.cleanup();
    }

    private void connectWithZooKeeper() throws IOException, KeeperException, InterruptedException {
        zk = new StateWatchingZooKeeper(zkConnectionString, 30000);

        final String zkRoot = conf.get("hbaseindexer.zookeeper.znode.parent");

        boolean indexerNodeExists = zk.retryOperation(new ZooKeeperOperation<Boolean>() {
            @Override
            public Boolean execute() throws KeeperException, InterruptedException {
                return zk.exists(zkRoot, false) != null;
            }
        });

        if (!indexerNodeExists) {
            System.err.println();
            System.err.println("WARNING: No " + zkRoot + " node found in ZooKeeper.");
            System.err.println();
        }
    }

    protected String getZkConnectionString() {
        return zkConnectionString;
    }

}
