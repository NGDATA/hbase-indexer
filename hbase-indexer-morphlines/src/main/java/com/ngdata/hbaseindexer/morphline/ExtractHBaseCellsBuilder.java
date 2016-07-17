/*
 * Copyright 2013 Cloudera Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer.morphline;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Fields;
import org.kitesdk.morphline.base.Validator;

import com.google.common.base.Preconditions;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMappers;
import com.ngdata.hbaseindexer.parse.extract.PrefixMatchingCellExtractor;
import com.ngdata.hbaseindexer.parse.extract.PrefixMatchingQualifierExtractor;
import com.ngdata.hbaseindexer.parse.extract.RowkeyExtractor;
import com.ngdata.hbaseindexer.parse.extract.SingleCellExtractor;
import com.typesafe.config.Config;

/**
 * Command that extracts cells from the given HBase Result, and transforms the resulting values into a
 * SolrInputDocument.
 */
public final class ExtractHBaseCellsBuilder implements CommandBuilder {

    @Override
    public Collection<String> getNames() {
        return Collections.singletonList("extractHBaseCells");
    }

    @Override
    public Command build(Config config, Command parent, Command child, MorphlineContext context) {
        return new ExtractHBaseCells(this, config, parent, child, context);
    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private static final class ExtractHBaseCells extends AbstractCommand {

        private final List<AbstractMapping> mappings = new ArrayList();

        public ExtractHBaseCells(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
            super(builder, config, parent, child, context);
            for (Config mappingConfig : getConfigs().getConfigList(config, "mappings")) {
                Configs configs = new Configs();
                LowerCaseValueSource source = new Validator<LowerCaseValueSource>().validateEnum(config,
                        configs.getString(mappingConfig, "source", LowerCaseValueSource.value.toString()),
                        LowerCaseValueSource.class);
                if (source == LowerCaseValueSource.value) {
                    mappings.add(new ValueMapping(mappingConfig, configs, context));
                } else if (source == LowerCaseValueSource.qualifier) {
                    mappings.add(new QualifierMapping(mappingConfig, configs, context));
                } else {
                    mappings.add(new RowkeyMapping(mappingConfig, configs, context));
                }
            }
            validateArguments();
        }

        @Override
        protected boolean doProcess(Record record) {
            Result result = (Result)record.getFirstValue(Fields.ATTACHMENT_BODY);
            Preconditions.checkNotNull(result);
            removeAttachments(record);
            for (AbstractMapping mapping : mappings) {
                mapping.apply(result, record);
            }
            // pass record to next command in chain:      
            return super.doProcess(record);
        }

        private void removeAttachments(Record outputRecord) {
            outputRecord.removeAll(Fields.ATTACHMENT_BODY);
            outputRecord.removeAll(Fields.ATTACHMENT_MIME_TYPE);
            outputRecord.removeAll(Fields.ATTACHMENT_CHARSET);
            outputRecord.removeAll(Fields.ATTACHMENT_NAME);
        }

    }

    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    private abstract static class AbstractMapping {

        protected final String outputField;
        protected final String type;
        protected final ByteArrayValueMapper byteArrayMapper;
        
        protected ByteArrayExtractor extractor; // must be set in inheritor
        
        public AbstractMapping(Config config, Configs configs) {
            this.outputField = configs.getString(config, "outputField", null);
            
            this.type = configs.getString(config, "type", "byte[]");
            if (type.equals("byte[]")) { // pass through byte[] to downstream morphline commands without conversion
                this.byteArrayMapper = new ByteArrayValueMapper() {
                    @Override
                    public Collection<byte[]> map(byte[] input) {
                        return Collections.singletonList(input);
                    }
                };
            } else {
                this.byteArrayMapper = ByteArrayValueMappers.getMapper(type);
            }
        }
        
        protected void afterInit(Config config, Configs configs, MorphlineContext context) {
            configs.validateArguments(config);
            if (this.extractor == null) {
                throw new IllegalStateException("Extractor has not been initialized");
            }
            if (context instanceof HBaseMorphlineContext) {
                ((HBaseMorphlineContext)context).getExtractors().add(this.extractor);
            }
        }
        
        protected void extractWithSingleOutputField(Result result, Record record) {
            Iterator<byte[]> iter = extractor.extract(result).iterator();
            while (iter.hasNext()) {
                for (Object value : byteArrayMapper.map(iter.next())) {
                    record.put(outputField, value);
                }
            }
        }
        
        public abstract void apply(Result result, Record record);
    }

    private abstract static class AbstractColumnMapping extends AbstractMapping {

        protected final String inputColumn;
        protected final byte[] columnFamily;
        protected final byte[] qualifier;
        protected final boolean isWildCard;
        protected final String outputFieldName;
        protected final List<String> outputFieldNames;
        protected final boolean isDynamicOutputFieldName;

        public AbstractColumnMapping(Config config, Configs configs) {
            super(config, configs);

            this.inputColumn = resolveColumnName(configs.getString(config, "inputColumn"));
            this.columnFamily = Bytes.toBytes(splitFamilyAndQualifier(inputColumn)[0]);

            String qualifierString = splitFamilyAndQualifier(inputColumn)[1];
            this.isWildCard = qualifierString.endsWith("*");
            if (isWildCard) {
                qualifierString = qualifierString.substring(0, qualifierString.length() - 1);
            }
            this.qualifier = Bytes.toBytes(qualifierString);

            this.outputFieldNames = configs.getStringList(config, "outputFields", null);
            if (outputField == null && outputFieldNames == null) {
                throw new MorphlineCompilationException("Either outputField or outputFields must be defined", config);
            }
            if (outputField != null && outputFieldNames != null) {
                throw new MorphlineCompilationException(
                        "Must not define both outputField and outputFields at the same time", config);
            }
            if (outputField == null) {
                this.isDynamicOutputFieldName = false;
                this.outputFieldName = null;
            } else {
                this.isDynamicOutputFieldName = outputField.endsWith("*");
                if (isDynamicOutputFieldName) {
                    this.outputFieldName = outputField.substring(0, outputField.length() - 1);
                } else {
                    this.outputFieldName = outputField;
                }
            }
        }
        
        @Override
        public void apply(Result result, Record record) {
            if (outputFieldNames != null) {
                extractWithMultipleOutputFieldNames(result, record);
            } else if (isDynamicOutputFieldName && isWildCard) {
                extractWithDynamicOutputFieldNames(result, record);
            } else {
                extractWithSingleOutputField(result, record);
            }
        }
        
        @Override
        protected void extractWithSingleOutputField(Result result, Record record) {
            Iterator<byte[]> iter = extractor.extract(result).iterator();
            while (iter.hasNext()) {
                for (Object value : byteArrayMapper.map(iter.next())) {
                    record.put(outputFieldName, value);
                }
            }
        }
        /**
         * Override for custom name resolution, if desired. For example you could override this to translate human
         * readable names to Kiji-encoded names.
         */
        protected String resolveColumnName(String inputColumn) {
            return inputColumn;
        }
        
        private void extractWithMultipleOutputFieldNames(Result result, Record record) {
            Iterator<byte[]> iter = extractor.extract(result).iterator();
            for (int i = 0; i < outputFieldNames.size() && iter.hasNext(); i++) {
                byte[] input = iter.next();
                String outputField = outputFieldNames.get(i);
                if (outputField.length() > 0) { // empty column name indicates omit this field on output
                    for (Object value : byteArrayMapper.map(input)) {
                        record.put(outputField, value);
                    }
                }
            }
        }
        
        private void extractWithDynamicOutputFieldNames(Result result, Record record) {
          Iterator<byte[]> iter = extractor.extract(result).iterator();
          NavigableMap<byte[], byte[]> qualifiersToValues = result.getFamilyMap(columnFamily);
          if (qualifiersToValues != null) {
              for (byte[] matchingQualifier : qualifiersToValues.navigableKeySet().tailSet(qualifier)) {
                  if (Bytes.startsWith(matchingQualifier, qualifier)) {
                      byte[] tail = Bytes.tail(matchingQualifier, matchingQualifier.length - qualifier.length);
                      String outputField = outputFieldName + Bytes.toString(tail);                        
                      for (Object value : byteArrayMapper.map(iter.next())) {
                          record.put(outputField, value);
                      }
                  } else {
                      break;
                  }
              }
              assert !iter.hasNext();
          }
        }
        
        private static String[] splitFamilyAndQualifier(String fieldValueExpression) {
            String[] splits = fieldValueExpression.split(":", 2);
            if (splits.length != 2) {
                throw new IllegalArgumentException("Invalid field value expression: " + fieldValueExpression);
            }
            return splits;
        }
    }

    private static class QualifierMapping extends AbstractColumnMapping {

        public QualifierMapping(Config config, Configs configs, MorphlineContext context) {
            super(config, configs);
            if (isWildCard) {
                this.extractor = new PrefixMatchingQualifierExtractor(columnFamily, qualifier);
            } else {
                throw new IllegalArgumentException("Can't create a non-prefix-based qualifier extractor");
            }
            afterInit(config, configs, context);
        }
    }

    private static class ValueMapping extends AbstractColumnMapping {
        public ValueMapping(Config config, Configs configs, MorphlineContext context) {
            super(config, configs);
            if (isWildCard) {
                this.extractor = new PrefixMatchingCellExtractor(columnFamily, qualifier);
            } else {
                this.extractor = new SingleCellExtractor(columnFamily, qualifier);
            }
            afterInit(config, configs, context);
        }
    }

    private static class RowkeyMapping extends AbstractMapping {

        public RowkeyMapping(Config config, Configs configs, MorphlineContext context) {
            super(config, configs);
            this.extractor = new RowkeyExtractor();
            afterInit(config, configs, context);
        }

        public void apply(Result result, Record record) {
            extractWithSingleOutputField(result, record);
        }
    }


    // /////////////////////////////////////////////////////////////////////////////
    // Nested classes:
    // /////////////////////////////////////////////////////////////////////////////
    /**
     * Specifies where values to index should be extracted from in an HBase {@code KeyValue}.
     */
    private static enum LowerCaseValueSource {
        /**
         * Extract values to index from the column qualifier of a {@code KeyValue}.
         */
        qualifier, // we expect lowercase in config file!

        /**
         * Extract values to index from the cell value of a {@code KeyValue}.
         */
        value, // we expect lowercase in config file!

        /**
         * Extract values to index from row-key bytes.
         */
        rowkey // we expect lowercase in config file!
    }
}
