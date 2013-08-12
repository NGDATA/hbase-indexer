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
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.cdk.morphline.api.Command;
import com.cloudera.cdk.morphline.api.CommandBuilder;
import com.cloudera.cdk.morphline.api.MorphlineContext;
import com.cloudera.cdk.morphline.api.Record;
import com.cloudera.cdk.morphline.base.AbstractCommand;
import com.cloudera.cdk.morphline.base.Configs;
import com.cloudera.cdk.morphline.base.Fields;
import com.cloudera.cdk.morphline.base.Validator;
import com.google.common.base.Preconditions;
import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMappers;
import com.ngdata.hbaseindexer.parse.extract.PrefixMatchingCellExtractor;
import com.ngdata.hbaseindexer.parse.extract.PrefixMatchingQualifierExtractor;
import com.ngdata.hbaseindexer.parse.extract.SingleCellExtractor;
import com.typesafe.config.Config;

/**
 * Command that extracts cells from the given HBase Result, and transforms the resulting values into
 * a SolrInputDocument.
 */
public final class ExtractHBaseCellsBuilder implements CommandBuilder {

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("extractHBaseCells");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new ExtractHBaseCells(config, parent, child, context);
  }
  
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class ExtractHBaseCells extends AbstractCommand {

    private final List<Mapping> mappings = new ArrayList();
  
    public ExtractHBaseCells(Config config, Command parent, Command child, MorphlineContext context) {
      super(config, parent, child, context);      
      for (Config mapping : getConfigs().getConfigList(config, "mappings")) {
        mappings.add(new Mapping(mapping, context));
      }
      validateArguments();
    }
        
    @Override
    protected boolean doProcess(Record record) {
      Result result = (Result) record.getFirstValue(Fields.ATTACHMENT_BODY);
      Preconditions.checkNotNull(result);
      removeAttachments(record);
      for (Mapping mapping : mappings) {
        mapping.apply(result, record);
      }
      return super.doProcess(record);
    }
    
    private void removeAttachments(Record outputRecord) {
      outputRecord.removeAll(Fields.ATTACHMENT_BODY);
      outputRecord.removeAll(Fields.ATTACHMENT_MIME_TYPE);
      outputRecord.removeAll(Fields.ATTACHMENT_CHARSET);
      outputRecord.removeAll(Fields.ATTACHMENT_NAME);
    }
    
  }

  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class Mapping {

    private final String inputColumn;
    private final byte[] columnFamily;
    private final byte[] qualifier;
    private final boolean isWildCard;
    private final String outputFieldName;
    private final ByteArrayExtractor extractor;
    private final String type;
    private final ByteArrayValueMapper byteArrayMapper;

    // also see ByteArrayExtractors
    public Mapping(Config config, MorphlineContext context) {
      Configs configs = new Configs();
      this.inputColumn = resolveColumnName(configs.getString(config, "inputColumn"));
      this.columnFamily = Bytes.toBytes(splitFamilyAndQualifier(inputColumn)[0]); 
      String qualifierString = splitFamilyAndQualifier(inputColumn)[1];
      this.isWildCard = qualifierString.endsWith("*");
      if (isWildCard) {
        qualifierString = qualifierString.substring(0, qualifierString.length() - 1);
      }
      this.qualifier = Bytes.toBytes(qualifierString);
      this.outputFieldName = configs.getString(config, "outputField");
      this.type = configs.getString(config, "type", "byte[]");
      if (type.equals("byte[]")) { // pass through byte[] to downstream morphline commands without conversion
        this.byteArrayMapper = new ByteArrayValueMapper() {          
          @Override
          public Collection map(byte[] input) {
            return Collections.singletonList(input);
          }
        };
      } else {
        this.byteArrayMapper = ByteArrayValueMappers.getMapper(type);
      }
      
      LowerCaseValueSource source = new Validator<LowerCaseValueSource>().validateEnum(
          config,
          configs.getString(config, "source", LowerCaseValueSource.value.toString()),
          LowerCaseValueSource.class);

      if (source == LowerCaseValueSource.value) {
        if (isWildCard) {
          this.extractor = new PrefixMatchingCellExtractor(columnFamily, qualifier);
        } else {
          this.extractor = new SingleCellExtractor(columnFamily, qualifier);
        }
      } else {
        if (isWildCard) {
          this.extractor = new PrefixMatchingQualifierExtractor(columnFamily, qualifier);
        } else {
          throw new IllegalArgumentException("Can't create a non-prefix-based qualifier extractor");
        }
      }
      if (context instanceof HBaseMorphlineContext) {
        ((HBaseMorphlineContext) context).getExtractors().add(this.extractor);
      }

      configs.validateArguments(config);
    }
    
    /**
     * Override for custom name resolution, if desired. For example you could override this to
     * translate human readable names to Kiji-encoded names.
     */
    protected String resolveColumnName(String inputColumn) {
      return inputColumn;
    }
    
    public void apply(Result result, Record record) {
      for (byte[] bytes : extractor.extract(result)) {
        for (Object value : byteArrayMapper.map(bytes)) {
          record.put(outputFieldName, value);
        }
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
    
  
  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
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
    value // we expect lowercase in config file!
  }
}
