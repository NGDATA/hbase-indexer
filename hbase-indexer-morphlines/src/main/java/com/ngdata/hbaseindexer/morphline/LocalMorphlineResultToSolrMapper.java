/*
 * Copyright 2013 Cloudera Inc.
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
package com.ngdata.hbaseindexer.morphline;

import static com.ngdata.sep.impl.HBaseShims.newGet;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableSet;
import java.util.TreeMap;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.MorphlineCompilationException;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.Compiler;
import com.cloudera.cdk.morphline.base.FaultTolerance;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Notifications;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs Result to Solr mapping using morphlines.
 * <p>
 * This class is not thread-safe.
 */
final class LocalMorphlineResultToSolrMapper implements ResultToSolrMapper, Configurable {

    private HBaseMorphlineContext morphlineContext;
    private Command morphline;
    private String morphlineFileAndId;
    private Timer mappingTimer;
    private final Collector collector = new Collector();
    private boolean isSafeMode = false; // safe but slow (debug-only)

    /**
     * Information to be used for constructing a Get to fetch data required for indexing.
     */
    private Map<byte[], NavigableSet<byte[]>> familyMap;

    private static final Logger LOG = LoggerFactory.getLogger(LocalMorphlineResultToSolrMapper.class);

    public LocalMorphlineResultToSolrMapper() {
    }

    @Override
    public void configure(Map<String, String> params) {
        if (LOG.isTraceEnabled()) {
            LOG.trace("CWD is {}", new File(".").getAbsolutePath());
            LOG.trace("Configuration:\n{}", Joiner.on("\n").join(new TreeMap(params).entrySet()));
        }

        FaultTolerance faultTolerance = new FaultTolerance(getBooleanParameter(FaultTolerance.IS_PRODUCTION_MODE,
                false, params), getBooleanParameter(FaultTolerance.IS_IGNORING_RECOVERABLE_EXCEPTIONS, false, params),
                getStringParameter(FaultTolerance.RECOVERABLE_EXCEPTION_CLASSES, SolrServerException.class.getName(),
                        params));

        this.morphlineContext = (HBaseMorphlineContext)new HBaseMorphlineContext.Builder().setExceptionHandler(
                faultTolerance).setMetricRegistry(new MetricRegistry()).build();

        String morphlineFile = params.get(MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM);
        String morphlineId = params.get(MorphlineResultToSolrMapper.MORPHLINE_ID_PARAM);
        if (morphlineFile == null || morphlineFile.trim().length() == 0) {
            throw new MorphlineCompilationException("Missing parameter: "
                        + MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM, null);
        }
        Map morphlineVariables = new HashMap();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String variablePrefix = MorphlineResultToSolrMapper.MORPHLINE_VARIABLE_PARAM + ".";
            if (entry.getKey().startsWith(variablePrefix)) {
                morphlineVariables.put(entry.getKey().substring(variablePrefix.length()), entry.getValue());
            }
        }
        Config override = ConfigFactory.parseMap(morphlineVariables);
        this.morphline = new Compiler().compile(new File(morphlineFile), morphlineId, morphlineContext, collector,
                override);
        this.morphlineFileAndId = morphlineFile + "@" + morphlineId;

        // precompute familyMap; see DefaultResultToSolrMapper ctor
        Get get = newGet();
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            byte[] columnFamily = extractor.getColumnFamily();
            byte[] columnQualifier = extractor.getColumnQualifier();
            if (columnFamily != null) {
                if (columnQualifier != null) {
                    get.addColumn(columnFamily, columnQualifier);
                } else {
                    get.addFamily(columnFamily);
                }
            }
        }
        this.familyMap = get.getFamilyMap();

        this.isSafeMode = getBooleanParameter("isSafeMode", false, params); // intentionally undocumented, not a public
                                                                            // API

        String metricName = MetricRegistry.name(getClass(), "HBase Result to Solr mapping time");
        this.mappingTimer = morphlineContext.getMetricRegistry().timer(metricName);
        Notifications.notifyBeginTransaction(morphline);
    }

    @Override
    public boolean containsRequiredData(Result result) {
        if (isSafeMode) {
            return false;
        }
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            if (!extractor.containsTarget(result)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        if (isSafeMode) {
            return true;
        }
        for (ByteArrayExtractor extractor : morphlineContext.getExtractors()) {
            if (extractor.isApplicable(kv)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Get getGet(byte[] row) {
        Get get = new Get(row);
        if (isSafeMode) {
            return get;
        }
        for (Entry<byte[], NavigableSet<byte[]>> familyMapEntry : familyMap.entrySet()) {
            // see DefaultResultToSolrMapper
            byte[] columnFamily = familyMapEntry.getKey();
            if (familyMapEntry.getValue() == null) {
                get.addFamily(columnFamily);
            } else {
                for (byte[] qualifier : familyMapEntry.getValue()) {
                    get.addColumn(columnFamily, qualifier);
                }
            }
        }
        return get;
    }

    @Override
    public SolrInputDocument map(Result result) {
        Timer.Context timerContext = mappingTimer.time();
        try {
            Record record = new Record();
            record.put(Fields.ATTACHMENT_BODY, result);
            record.put(Fields.ATTACHMENT_MIME_TYPE, MorphlineResultToSolrMapper.OUTPUT_MIME_TYPE);
            collector.reset();
            try {
                Notifications.notifyStartSession(morphline);
                if (!morphline.process(record)) {
                    LOG.warn("Morphline {} failed to process record: {}", morphlineFileAndId, record);
                }
            } catch (RuntimeException t) {
                morphlineContext.getExceptionHandler().handleException(t, record);
            }

            List<Record> results = collector.getRecords();
            if (results.size() == 0) {
                return null;
            }
            if (results.size() > 1) {
                throw new IllegalStateException(getClass().getName()
                        + " must not generate more than one output record per input HBase Result event");
            }
            SolrInputDocument solrInputDocument = convert(results.get(0));
            return solrInputDocument;
        } finally {
            timerContext.stop();
        }
    }

    private SolrInputDocument convert(Record record) {
        Map<String, Collection<Object>> map = record.getFields().asMap();
        SolrInputDocument doc = new SolrInputDocument(new HashMap(2 * map.size()));
        for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
            doc.setField(entry.getKey(), entry.getValue());
        }
        return doc;
    }

    private boolean getBooleanParameter(String name, boolean defaultValue, Map<String, String> map) {
        String value = map.get(name);
        return value == null ? defaultValue : "TRUE".equalsIgnoreCase(value);
    }

    private String getStringParameter(String name, String defaultValue, Map<String, String> map) {
        String value = map.get(name);
        return value == null ? defaultValue : value;
    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private static final class Collector implements Command {

        private final List<Record> results = new ArrayList();

        public List<Record> getRecords() {
            return results;
        }

        public void reset() {
            results.clear();
        }

        @Override
        public Command getParent() {
            return null;
        }

        @Override
        public void notify(Record notification) {
        }

        @Override
        public boolean process(Record record) {
            Preconditions.checkNotNull(record);
            results.add(record);
            return true;
        }

    }

}
