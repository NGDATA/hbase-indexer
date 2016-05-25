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

import static com.ngdata.hbaseindexer.metrics.IndexerMetricsUtil.metricName;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.SolrInputDocument;

/**
 * Writes updates (new documents and deletes) directly to a set of SolrClient (one for each shard).
 * <p>
 * There are two main pieces of functionality that this class provides, both related to error handling in Solr:
 * <h3>Selective swallowing of errors</h3>
 * If a write to Solr throws an exception signifying that the underlying problem is related to Solr, then the exception
 * will be thrown up the stack. The intention of this behaviour is to allow the write to be re-tried indefinitely until
 * the Solr issue is resolved.
 * <p>
 * If a write to Solr throws an exception signifying that the underlying problem lies with the document being written,
 * then the exception will be logged, but otherwise ignored. The intention of this behaviour is to stop a single bad
 * document from holding up the whole indexing process for other documents.
 *
 * <h3>Individual retry of documents</h3>
 * If a single document in a batch causes an exception to be thrown that is related to the document itself, then each
 * update will be retried individually.
 */
public class DirectSolrClassicInputDocumentWriter implements SolrInputDocumentWriter {

    private Log log = LogFactory.getLog(getClass());
    private List<SolrClient> solrServers;
    private Meter indexAddMeter;
    private Meter indexDeleteMeter;
    private Meter solrAddErrorMeter;
    private Meter solrDeleteErrorMeter;
    private Meter documentAddErrorMeter;
    private Meter documentDeleteErrorMeter;

    public DirectSolrClassicInputDocumentWriter(String indexName, List<SolrClient> solrServers) {
        this.solrServers = solrServers;

        indexAddMeter = Metrics.newMeter(metricName(getClass(), "Index adds", indexName), "Documents added to Solr index",
                TimeUnit.SECONDS);
        indexDeleteMeter = Metrics.newMeter(metricName(getClass(), "Index deletes", indexName),
                "Documents deleted from Solr index", TimeUnit.SECONDS);
        solrAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr add errors", indexName),
                "Documents not added to Solr due to Solr errors", TimeUnit.SECONDS);
        solrDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr delete errors", indexName),
                "Documents not deleted from Solr due to Solr errors", TimeUnit.SECONDS);
        documentAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Document add errors", indexName),
                "Documents not added to Solr due to document errors", TimeUnit.SECONDS);
        documentDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Document delete errors", indexName),
                "Documents not deleted from Solr due to document errors", TimeUnit.SECONDS);

    }

    private boolean isDocumentIssue(SolrException e) {
        return e.code() == ErrorCode.BAD_REQUEST.code;
    }

    private void logOrThrowSolrException(SolrException solrException) {
        if (isDocumentIssue(solrException)) {
            log.error("Error updating Solr", solrException);
        } else {
            throw solrException;
        }
    }

    /**
     * Write a list of documents to Solr.
     * <p>
     * If a server occurs while writing the update, the exception will be thrown up the stack. If one or more of the
     * documents contain issues, the error will be logged and swallowed, with all other updates being performed.
     */
    @Override
    public void add(int shard, Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException {
        Collection<SolrInputDocument> inputDocuments = inputDocumentMap.values();
        try {
            solrServers.get(shard).add(inputDocuments);
            indexAddMeter.mark(inputDocuments.size());
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                retryAddsIndividually(shard, inputDocuments);
            } else {
                solrAddErrorMeter.mark(inputDocuments.size());
                throw e;
            }
        } catch (SolrServerException sse) {
            solrAddErrorMeter.mark(inputDocuments.size());
            throw sse;
        }
    }

    private void retryAddsIndividually(int shard, Collection<SolrInputDocument> inputDocuments) throws SolrServerException,
            IOException {
        for (SolrInputDocument inputDocument : inputDocuments) {
            try {
                solrServers.get(shard).add(inputDocument);
                indexAddMeter.mark();
            } catch (SolrException e) {
                logOrThrowSolrException(e);
                // No exception thrown through, so we can update the metric
                documentAddErrorMeter.mark();
            }
        }
    }

    /**
     * Delete a list of documents ids from Solr.
     * <p>
     * If a server occurs while performing the delete, the exception will be thrown up the stack. If one or more of the
     * deletes cause issues, the error will be logged and swallowed, with all other updates being performed.
     */
    @Override
    public void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
        try {
            solrServers.get(shard).deleteById(idsToDelete);
            indexDeleteMeter.mark(idsToDelete.size());
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                retryDeletesIndividually(shard, idsToDelete);
            } else {
                solrDeleteErrorMeter.mark(idsToDelete.size());
                throw e;
            }
        } catch (SolrServerException sse) {
            solrDeleteErrorMeter.mark(idsToDelete.size());
            throw sse;
        }
    }

    private void retryDeletesIndividually(int shard, List<String> idsToDelete) throws SolrServerException, IOException {
        for (String idToDelete : idsToDelete) {
            try {
                solrServers.get(shard).deleteById(idToDelete);
                indexDeleteMeter.mark();
            } catch (SolrException e) {
                logOrThrowSolrException(e);
                // No exception thrown through, so we can update the metric
                documentDeleteErrorMeter.mark();
            }
        }
    }

    /**
     * Has the same behavior as {@link org.apache.solr.client.solrj.SolrClient#deleteByQuery(String)}.
     *
     * @param deleteQuery delete query to be executed
     */
    @Override
    public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
        try {
            for (SolrClient server : solrServers) {
                server.deleteByQuery(deleteQuery);
            }
        } catch (SolrException e) {
            if (isDocumentIssue(e)) {
                documentDeleteErrorMeter.mark(1);
            } else {
                solrDeleteErrorMeter.mark(1);
                throw e;
            }
        } catch (SolrServerException sse) {
            solrDeleteErrorMeter.mark(1);
            throw sse;
        }
    }

    @Override
    public void close() {
        for (SolrClient server : solrServers) {
            server.shutdown();
        }
    }

    public int getNumServers() {
        return solrServers.size();
    }
}
