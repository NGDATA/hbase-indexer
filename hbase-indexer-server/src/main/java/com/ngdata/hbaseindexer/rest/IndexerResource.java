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
package com.ngdata.hbaseindexer.rest;

import javax.servlet.ServletContext;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactory;
import com.ngdata.hbaseindexer.conf.IndexerComponentFactoryUtil;
import com.ngdata.hbaseindexer.indexer.Indexer;
import com.ngdata.hbaseindexer.indexer.RowData;
import com.ngdata.hbaseindexer.model.api.IndexerDefinition;
import com.ngdata.hbaseindexer.model.api.IndexerDefinitionBuilder;
import com.ngdata.hbaseindexer.model.api.IndexerNotFoundException;
import com.ngdata.hbaseindexer.model.api.WriteableIndexerModel;
import com.ngdata.hbaseindexer.model.impl.IndexerDefinitionJsonSerDeser;
import com.ngdata.hbaseindexer.supervisor.IndexerSupervisor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

@Path("indexer")
public class IndexerResource {
    @Context
    protected ServletContext servletContext;

    /**
     * Get all index definitions.
     */
    @GET
    @Produces("application/json")
    public Collection<IndexerDefinition> get(@Context UriInfo uriInfo) {
        return getModel().getIndexers();
    }

    /**
     * Get a single index definition.
     */
    @GET
    @Path("{name}")
    @Produces("application/json")
    public IndexerDefinition get(@PathParam("name") String name) throws IndexerNotFoundException {
        return getModel().getIndexer(name);
    }

    /**
     * Get a single index configuration (as stored in zookeeper).
     */
    @GET
    @Path("{name}/config")
    @Produces("application/json")
    public Response getConfig(@PathParam("name") String name) throws IndexerNotFoundException, IOException {
        IndexerDefinition index = getModel().getIndexer(name);

        ObjectMapper m = new ObjectMapper();
        ObjectNode json = m.createObjectNode();
        json.put("occVersion", index.getOccVersion());
        json.put("config", new String(index.getConfiguration(), Charsets.UTF_8));

        return Response.ok(m.writeValueAsString(json), new MediaType("application", "json")).build();
    }

    /**
     * Update an index definition.
    */
    @PUT
    @Path("{name}")
    @Consumes("application/json")
    @Produces("application/json")
    public IndexerDefinition put(@PathParam("name") String indexName, ObjectNode json) throws Exception {
        WriteableIndexerModel model = getModel();
        ObjectMapper m = new ObjectMapper();

        IndexerDefinition oldIndexer = model.getIndexer(indexName);
        IndexerDefinition indexerDefinition = IndexerDefinitionJsonSerDeser.INSTANCE.fromJson(json,
                new IndexerDefinitionBuilder().startFrom(oldIndexer)).build();

        IndexerDefinition.LifecycleState lifecycleState = json.has("lifecycleState") ?
                IndexerDefinition.LifecycleState.valueOf(json.get("lifecycleState").getTextValue()) : null;

        String lock = model.lockIndexer(indexName);
        try {
            if (!oldIndexer.equals(indexerDefinition)) {
                model.updateIndexer(indexerDefinition, lock);
                //System.out.println("Index updated: " + indexName);
            } else {
                //System.out.println("Index already matches the specified settings, did not update it.");
            }
        } finally {
            // In case we requested deletion of an index, it might be that the lock is already removed
            // by the time we get here as part of the index deletion.
            boolean ignoreMissing = lifecycleState != null && lifecycleState == IndexerDefinition.LifecycleState.DELETE_REQUESTED;
            model.unlockIndexer(lock, ignoreMissing);
        }

        return indexerDefinition;
    }


    /**
     * Trigger indexing of a record on the specified index.
     */
    @POST
    @Path("{name}")
    public void indexOn(@QueryParam("action") String action, @PathParam("name") String indexName,
                        @QueryParam("id")final String rowkey, @QueryParam("table") String tableName) throws Exception {
        if ("index".equals(action)) {            
            Indexer indexer = getIndexerSupervisor().getIndexer(indexName);
            List<RowData> rowData = new ArrayList<RowData>();

            if (tableName != null || tableName.isEmpty()) {
                tableName = fetchIndexerTableName(indexName);
            }

            rowData.add(new KeyRowData(rowkey.getBytes(Charsets.UTF_8), tableName.getBytes(Charsets.UTF_8)));
            indexer.indexRowData(rowData);            
        } else {
            throw new WebApplicationException(
                    Response.status(Response.Status.BAD_REQUEST).entity("Unsupported POST action: " + action).build());
        }
    }

    private String fetchIndexerTableName(String indexerName) throws Exception{
        // best effort since this could be a pattern ...
        IndexerDefinition indexerDefinition = get(indexerName);
        IndexerComponentFactory factory = IndexerComponentFactoryUtil.getComponentFactory(indexerDefinition.getIndexerComponentFactory(), new ByteArrayInputStream(indexerDefinition.getConfiguration()), indexerDefinition.getConnectionParams());
        String tableName = factory.createIndexerConf().getTable();

        // TODO we should fail if the table does not exist
        return tableName;
    }
     

    /**
     * Trigger indexing of a record on specified or all matching indexes.
     */
    @POST
    @Path("")
    public void index(@QueryParam("action") String action, @QueryParam("indexes") String commaSeparatedIndexNames,
                      @QueryParam("id") String rowKey, @QueryParam("table") String tableName) throws Exception {
        IndexerSupervisor indexerSupervisor = getIndexerSupervisor();
        Set<String> indexNames = parse(commaSeparatedIndexNames);
        if (indexNames.isEmpty()) {
            indexNames = indexerSupervisor.getRunningIndexers();
        }

        for (String indexName : indexNames) {
            indexOn(action, indexName, rowKey, tableName);
        }
    }

    private Set<String> parse(String commaSeparatedIndexNames) {
        final Set<String> results = new HashSet<String>();
        if (commaSeparatedIndexNames != null) {
            for (String name : Splitter.on(',').omitEmptyStrings().trimResults().split(commaSeparatedIndexNames)) {
                results.add(name);
            }
        }
        return results;
    }

    private WriteableIndexerModel getModel() {
        return ((WriteableIndexerModel)servletContext.getAttribute("indexerModel"));
    }

    private IndexerSupervisor getIndexerSupervisor() {
        return (IndexerSupervisor)servletContext.getAttribute("indexerSupervisor");
    }
    
    private static class KeyRowData implements RowData {
        private byte[] rowKey;
        private byte[] tableName;

        public KeyRowData(byte[] rowKey, byte[] tableName) {
            this.rowKey = rowKey;
            this.tableName = tableName;
        }
        
        @Override
        public List<KeyValue> getKeyValues() {            
            return Lists.newArrayList(new KeyValue(rowKey));
        }

        @Override
        public byte[] getRow() {
            return rowKey;
        }

        @Override
        public Result toResult() {            
            return new Result(getKeyValues());
        }

        @Override
        public byte[] getTable() {
            return tableName;
        }
    }
}
