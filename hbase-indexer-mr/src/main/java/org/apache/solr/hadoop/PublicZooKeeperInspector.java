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
package org.apache.solr.hadoop;

import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.zookeeper.KeeperException;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Nasty trick to access the non public {@link ZooKeeperInspector} class.
 */
public class PublicZooKeeperInspector {
    private static final ZooKeeperInspector DELEGATE = new ZooKeeperInspector();

    public static List<List<String>> extractShardUrls(String zkHost, String collection) {
        return DELEGATE.extractShardUrls(zkHost, collection);
    }

    public static String readConfigName(SolrZkClient zkClient, String collection) throws KeeperException, InterruptedException {
        return DELEGATE.readConfigName(zkClient, collection);
    }

    public static DocCollection extractDocCollection(String zkHost, String collection) {
        return DELEGATE.extractDocCollection(zkHost, collection);
    }

    public static List<Slice> getSortedSlices(Collection<Slice> slices) {
        return DELEGATE.getSortedSlices(slices);
    }

    public static SolrZkClient getZkClient(String zkHost) {
        return DELEGATE.getZkClient(zkHost);
    }

    public static File downloadConfigDir(SolrZkClient zkClient, String configName) throws IOException, InterruptedException, KeeperException {
        return DELEGATE.downloadConfigDir(zkClient, configName);
    }
}
