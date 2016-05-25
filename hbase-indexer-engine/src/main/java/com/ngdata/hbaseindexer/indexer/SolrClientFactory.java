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

package com.ngdata.hbaseindexer.indexer;

import java.lang.reflect.InvocationTargetException;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.SolrConnectionParams;
import com.ngdata.hbaseindexer.util.solr.SolrConnectionParamUtil;
import org.apache.http.client.HttpClient;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * Create cloud or classic {@link SolrClient} instances from a map of solr connection parameters.
 */
public class SolrClientFactory {
    public static SolrClient createCloudSolrClient(Map<String, String> connectionParameters, int zkSessionTimeout) throws MalformedURLException {
        String solrZk = connectionParameters.get(SolrConnectionParams.ZOOKEEPER);
        CloudSolrClient solr = new CloudSolrClient(solrZk);
        solr.setZkClientTimeout(zkSessionTimeout);
        solr.setZkConnectTimeout(zkSessionTimeout);      
        String collection = connectionParameters.get(SolrConnectionParams.COLLECTION);
        solr.setDefaultCollection(collection);
        return solr;
    }
    
    public static List<SolrClient> createHttpSolrClients(Map<String, String> connectionParams, HttpClient httpClient) {
        List<SolrClient> result = Lists.newArrayList();
        for (String shard : SolrConnectionParamUtil.getShards(connectionParams)) {
            result.add(new HttpSolrClient(shard, httpClient));
        }
        if (result.size() == 0) {
            throw new RuntimeException(
                    String.format("You need to specify at least one solr shard connection parameter (%s0={url})",
                            SolrConnectionParams.SOLR_SHARD_PREFIX));
        }
        return result;
    }

    public static Sharder createSharder(Map<String, String> connectionParams, int numShards) throws SharderException {
        String sharderType = connectionParams.get(SolrConnectionParams.SHARDER_TYPE);
        if (sharderType == null || sharderType.equals("default")) {
            return new HashSharder(numShards);
        } else {
            try {
                return (Sharder) Class.forName(sharderType).getConstructor(Integer.TYPE).newInstance(numShards);
            } catch (ClassNotFoundException e) {
                throw new SharderException("failed to initialize sharder", e);
            } catch (InvocationTargetException e) {
                throw new SharderException("failed to initialize sharder", e);
            } catch (NoSuchMethodException e) {
                throw new SharderException("failed to initialize sharder", e);
            } catch (InstantiationException e) {
                throw new SharderException("failed to initialize sharder", e);
            } catch (IllegalAccessException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                throw new SharderException("failed to initialize sharder", e);
            }
        }
    }

}
