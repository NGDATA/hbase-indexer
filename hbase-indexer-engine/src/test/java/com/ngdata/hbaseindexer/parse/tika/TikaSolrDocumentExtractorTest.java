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
package com.ngdata.hbaseindexer.parse.tika;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;

import com.ngdata.hbaseindexer.parse.SolrDocumentExtractor;
import com.ngdata.hbaseindexer.parse.extract.SingleCellExtractor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.IndexSchema;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.XHTMLContentHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class TikaSolrDocumentExtractorTest {
    
    private static IndexSchema indexSchema;
    
    @BeforeClass
    public static void setupBeforeClass() throws ParserConfigurationException, IOException, SAXException {
        InputSource configInputSource = new InputSource(TikaSolrDocumentExtractorTest.class.getResourceAsStream("/solrconfig.xml"));
        SolrConfig solrConfig = new SolrConfig("example", configInputSource);
        InputSource schemaInputSource = new InputSource(TikaSolrDocumentExtractorTest.class.getResourceAsStream("/schema.xml"));
        indexSchema = new IndexSchema(solrConfig, "schema.xml", schemaInputSource);
    }

    @Test
    public void testExtractDocument() throws IOException {

        byte[] columnFamily = Bytes.toBytes("cf");
        byte[] columnQualifier = Bytes.toBytes("qualifier");
        final String applicableValue = "this is the test data";
        final String nonApplicableValue = "not-applicable value";
        KeyValue applicableKeyValue = new KeyValue(Bytes.toBytes("row"), columnFamily, columnQualifier,
                Bytes.toBytes(applicableValue));
        KeyValue nonApplicableKeyValue = new KeyValue(Bytes.toBytes("row"), Bytes.toBytes("other cf"), columnQualifier,
                Bytes.toBytes(nonApplicableValue));
        Result result = new Result(new KeyValue[] { applicableKeyValue, nonApplicableKeyValue });

        SolrDocumentExtractor documentExtractor = new TikaSolrDocumentExtractor(indexSchema, new SingleCellExtractor(
                columnFamily, columnQualifier), "prefix_", "text/plain");
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        documentExtractor.extractDocument(result, solrInputDocument);

        // Make sure that the input text got in here somehow
        assertTrue(solrInputDocument.containsKey("prefix_content"));
        assertTrue(solrInputDocument.get("prefix_content").getValues().toString().contains(applicableValue));

        assertFalse(solrInputDocument.get("prefix_content").getValues().toString().contains(nonApplicableValue));
    }
    
    @Test
    public void testExtractDocument_CustomTikaParser() throws IOException {
        byte[] columnFamily = Bytes.toBytes("cf");
        byte[] columnQualifier = Bytes.toBytes("qualifier");
        final String applicableValue = "this is the test data";
        KeyValue kv = new KeyValue(Bytes.toBytes("row"), columnFamily, columnQualifier,
                Bytes.toBytes(applicableValue));
        Result result = new Result(new KeyValue[] { kv });

        SolrDocumentExtractor documentExtractor = new TikaSolrDocumentExtractor(indexSchema, new SingleCellExtractor(
                columnFamily, columnQualifier), null, "application/dummy");
        SolrInputDocument solrInputDocument = new SolrInputDocument();
        documentExtractor.extractDocument(result, solrInputDocument);

        // Just make sure that the data came through the dynamically-loaded Tika parser
        assertEquals(DummyParser.INDEX_VALUE, solrInputDocument.get(DummyParser.INDEX_FIELD).getFirstValue());
      
    }
    
    public static class DummyParser implements Parser {
        
        public static final MediaType DUMMY_MEDIA_TYPE = MediaType.application("dummy");
        public static final String DUMMY_MIME_TYPE = "application/dummy";

        public static final String INDEX_FIELD = "_index_field_";
        public static final String INDEX_VALUE = "_index_value_";
        

        @Override
        public Set<MediaType> getSupportedTypes(ParseContext context) {
            return Collections.singleton(DUMMY_MEDIA_TYPE);
        }

        @Override
        public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
                throws IOException, SAXException, TikaException {
            
            metadata.set(INDEX_FIELD, INDEX_VALUE);

            XHTMLContentHandler xhtml = new XHTMLContentHandler(handler, metadata);
            xhtml.startDocument();
            xhtml.endDocument();
        }
        
    }

}
