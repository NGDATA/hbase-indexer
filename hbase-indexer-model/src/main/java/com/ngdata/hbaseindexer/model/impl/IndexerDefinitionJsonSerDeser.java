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
package com.ngdata.hbaseindexer.model.impl;

import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.BatchIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.IncrementalIndexingState;
import static com.ngdata.hbaseindexer.model.api.IndexerDefinition.LifecycleState;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ngdata.hbaseindexer.model.api.BatchBuildInfo;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.util.json.JsonUtil;
import net.iharder.Base64;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;

public class IndexerDefinitionJsonSerDeser {
    public static IndexerDefinitionJsonSerDeser INSTANCE = new IndexerDefinitionJsonSerDeser();

    public IndexerDefinitionBuilder fromJsonBytes(byte[] json) {
        return fromJsonBytes(json, new IndexerDefinitionBuilder());
    }

    public IndexerDefinitionBuilder fromJsonBytes(byte[] json, IndexerDefinitionBuilder indexerDefinitionBuilder) {
        ObjectNode node;
        try {
            node = (ObjectNode) new ObjectMapper().readTree(new ByteArrayInputStream(json));
        } catch (IOException e) {
            throw new RuntimeException("Error parsing indexer definition JSON.", e);
        }
        return fromJson(node, indexerDefinitionBuilder);
    }

    public IndexerDefinitionBuilder fromJson(ObjectNode node) {
        return fromJson(node, new IndexerDefinitionBuilder());
    }

    public IndexerDefinitionBuilder fromJson(ObjectNode node, IndexerDefinitionBuilder indexerDefinitionBuilder) {
        String name = JsonUtil.getString(node, "name");
        LifecycleState lifecycleState = LifecycleState.valueOf(JsonUtil.getString(node, "lifecycleState"));
        IncrementalIndexingState incrementalIndexingState =
                IncrementalIndexingState.valueOf(JsonUtil.getString(node, "incrementalIndexingState"));
        BatchIndexingState batchIndexingState = BatchIndexingState.valueOf(JsonUtil.getString(node, "batchIndexingState"));

        String queueSubscriptionId = JsonUtil.getString(node, "subscriptionId", null);
        long subscriptionTimestamp = JsonUtil.getLong(node, "subscriptionTimestamp", 0L);

        String indexerComponentFactory = JsonUtil.getString(node, "indexerComponentFactory", null);

        byte[] configuration = getByteArrayProperty(node, "configuration");

        String connectionType = JsonUtil.getString(node, "connectionType", null);
        ObjectNode connectionParamsNode = JsonUtil.getObject(node, "connectionParams", null);
        Map<String, String> connectionParams = null;
        if (connectionParamsNode != null) {
            connectionParams = new HashMap<String, String>();
            Iterator<Map.Entry<String, JsonNode>> it = connectionParamsNode.getFields();
            while (it.hasNext()) {
                Map.Entry<String, JsonNode> entry = it.next();
                connectionParams.put(entry.getKey(), entry.getValue().getTextValue());
            }
        }

        BatchBuildInfo activeBatchBuild = null;
        if (node.get("activeBatchBuild") != null) {
            activeBatchBuild = parseBatchBuildInfo(JsonUtil.getObject(node, "activeBatchBuild"));
        }

        BatchBuildInfo lastBatchBuild = null;
        if (node.get("lastBatchBuild") != null) {
            lastBatchBuild = parseBatchBuildInfo(JsonUtil.getObject(node, "lastBatchBuild"));
        }

        String[] batchIndexCliArguments = getStringArrayProperty(node, "batchIndexCliArguments");
        String[] defaultBatchIndexCliArguments = getStringArrayProperty(node, "defaultBatchIndexCliArguments");

        int occVersion = JsonUtil.getInt(node, "occVersion");

        indexerDefinitionBuilder.name(name);
        indexerDefinitionBuilder.lifecycleState(lifecycleState);
        indexerDefinitionBuilder.incrementalIndexingState(incrementalIndexingState);
        indexerDefinitionBuilder.batchIndexingState(batchIndexingState);
        indexerDefinitionBuilder.subscriptionId(queueSubscriptionId);
        indexerDefinitionBuilder.subscriptionTimestamp(subscriptionTimestamp);
        indexerDefinitionBuilder.configuration(configuration);
        indexerDefinitionBuilder.indexerComponentFactory(indexerComponentFactory);
        indexerDefinitionBuilder.connectionType(connectionType);
        indexerDefinitionBuilder.connectionParams(connectionParams);
        indexerDefinitionBuilder.activeBatchBuildInfo(activeBatchBuild);
        indexerDefinitionBuilder.lastBatchBuildInfo(lastBatchBuild);
        indexerDefinitionBuilder.batchIndexCliArguments(batchIndexCliArguments);
        indexerDefinitionBuilder.defaultBatchIndexCliArguments(defaultBatchIndexCliArguments);
        indexerDefinitionBuilder.occVersion(occVersion);
        return indexerDefinitionBuilder;
    }

    private BatchBuildInfo parseBatchBuildInfo(ObjectNode buildNode) {

        Map<String, String> jobs = new HashMap<String, String>();
        ObjectNode jobsNode = JsonUtil.getObject(buildNode, "mapReduceJobTrackingUrls");
        Iterator<String> it = jobsNode.getFieldNames();
        while (it.hasNext()) {
            String key = it.next();
            String value = JsonUtil.getString(jobsNode, key);
            jobs.put(key, value);
        }

        BatchBuildInfo batchBuildInfo = new BatchBuildInfo(
                JsonUtil.getLong(buildNode, "submitTime"),
                JsonUtil.getBoolean(buildNode, "finishedSuccessful"),
                jobs,
                getStringArrayProperty(buildNode, "batchIndexCliArguments"));
        return batchBuildInfo;
    }

    private byte[] getByteArrayProperty(ObjectNode node, String property) {
        try {
            String string = JsonUtil.getString(node, property, null);
            if (string == null)
                return null;
            return Base64.decode(string);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void setByteArrayProperty(ObjectNode node, String property, byte[] data) {
        if (data == null)
            return;
        try {
            node.put(property, Base64.encodeBytes(data, Base64.GZIP));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] toJsonBytes(IndexerDefinition indexer) {
        try {
            return new ObjectMapper().writeValueAsBytes(toJson(indexer));
        } catch (IOException e) {
            throw new RuntimeException("Error serializing indexer definition to JSON.", e);
        }
    }

    private String[] getStringArrayProperty(ObjectNode node, String property) {
        ArrayNode arrayNode = JsonUtil.getArray(node, property, null);
        if (arrayNode == null)
            return null;
        else {
            List<String> strings = new ArrayList<String>();
            for (JsonNode jsonNode : arrayNode) {
                strings.add(jsonNode.getValueAsText());
            }

            return strings.toArray(new String[strings.size()]);
        }
    }

    private void setStringArrayProperty(ObjectNode node, String property, String[] strings) {
        if (strings != null) {
            ArrayNode arrayNode = node.putArray(property);
            for (String string : strings) {
                arrayNode.add(string);
            }
            node.put(property, arrayNode);
        }
    }

    public ObjectNode toJson(IndexerDefinition indexer) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();

        node.put("name", indexer.getName());
        node.put("lifecycleState", indexer.getLifecycleState().toString());
        node.put("batchIndexingState", indexer.getBatchIndexingState().toString());
        node.put("incrementalIndexingState", indexer.getIncrementalIndexingState().toString());

        node.put("occVersion", indexer.getOccVersion());

        if (indexer.getSubscriptionId() != null)
            node.put("subscriptionId", indexer.getSubscriptionId());

        node.put("subscriptionTimestamp", indexer.getSubscriptionTimestamp());

        if (indexer.getIndexerComponentFactory() != null) {
            node.put("indexerComponentFactory", indexer.getIndexerComponentFactory());
        }
        setByteArrayProperty(node, "configuration", indexer.getConfiguration());

        if (indexer.getConnectionType() != null)
            node.put("connectionType", indexer.getConnectionType());

        if (indexer.getConnectionParams() != null) {
            ObjectNode paramsNode = node.putObject("connectionParams");
            for (Map.Entry<String, String> entry : indexer.getConnectionParams().entrySet()) {
                paramsNode.put(entry.getKey(), entry.getValue());
            }
        }

        if (indexer.getActiveBatchBuildInfo() != null) {
            BatchBuildInfo buildInfo = indexer.getActiveBatchBuildInfo();
            ObjectNode batchNode = node.putObject("activeBatchBuild");

            setBatchBuildInfo(buildInfo, batchNode);
        }

        if (indexer.getLastBatchBuildInfo() != null) {
            BatchBuildInfo buildInfo = indexer.getLastBatchBuildInfo();
            ObjectNode batchNode = node.putObject("lastBatchBuild");

            setBatchBuildInfo(buildInfo, batchNode);
        }

        setStringArrayProperty(node, "batchIndexCliArguments", indexer.getBatchIndexCliArguments());
        setStringArrayProperty(node, "defaultBatchIndexCliArguments", indexer.getDefaultBatchIndexCliArguments());

        return node;
    }

    private void setBatchBuildInfo(BatchBuildInfo buildInfo, ObjectNode batchNode) {
        batchNode.put("submitTime", buildInfo.getSubmitTime());
        Boolean isFinishedSuccessful = buildInfo.isFinishedSuccessful();
        if (isFinishedSuccessful == null) {
            batchNode.put("finishedSuccessful", batchNode.nullNode());
        } else {
            batchNode.put("finishedSuccessful", isFinishedSuccessful);          
        }
        ObjectNode jobs = batchNode.putObject("mapReduceJobTrackingUrls");
        for (Map.Entry<String, String> entry : buildInfo.getMapReduceJobTrackingUrls().entrySet()) {
            jobs.put(entry.getKey(), entry.getValue());
        }
        setStringArrayProperty(batchNode, "batchIndexCliArguments", buildInfo.getBatchIndexCliArguments());
    }
}
