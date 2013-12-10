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
package com.ngdata.hbaseindexer.conf;

import com.google.common.collect.Iterators;
import org.codehaus.jackson.node.ObjectNode;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.OutputStream;
import java.net.URL;
import java.util.Map;

public class XmlIndexerConfWriter {
    public static void writeConf (IndexerConf conf, OutputStream os)
            throws SAXException, ParserConfigurationException, TransformerException{
        SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        URL url = XmlIndexerConfWriter.class.getResource("indexerconf.xsd");
        Schema schema = schemaFactory.newSchema(url);

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setSchema(schema);
        factory.setValidating(true);

        DocumentBuilder builder = factory.newDocumentBuilder();

        Document document = builder.newDocument();

        Element indexerEl = document.createElement("indexer");
        document.appendChild(indexerEl);
        indexerEl.setAttribute("table", conf.getTable());
        if (conf.getMappingType() != null) indexerEl.setAttribute("mapping-type",
                conf.getMappingType().toString().toLowerCase());
        if (conf.getRowReadMode() != null) indexerEl.setAttribute("read-row",
                conf.getRowReadMode().toString().toLowerCase());
        if (conf.getUniqueKeyField() != null) indexerEl.setAttribute("unique-key-field", conf.getUniqueKeyField());
        if (conf.getRowField() != null) indexerEl.setAttribute("row-field", conf.getRowField());
        if (conf.getColumnFamilyField() != null) indexerEl.setAttribute("column-family-field", conf.getColumnFamilyField());
        if (conf.getTableNameField() != null) indexerEl.setAttribute("table-name-field", conf.getTableNameField());

        Map<String, String> params = conf.getGlobalParams();
        addParams(params, indexerEl);

        if (conf.getMapperClass() != null) indexerEl.setAttribute("mapper", conf.getMapperClass().getName());
        if (conf.getUniqueKeyFormatterClass() != null) indexerEl.setAttribute("unique-key-formatter",
                conf.getUniqueKeyFormatterClass().getName());

        if (conf.getFieldDefinitions() != null) {
            for (FieldDefinition fieldDefinition : conf.getFieldDefinitions()) {
                Element fieldDefEl = document.createElement("field");
                indexerEl.appendChild(fieldDefEl);
                fieldDefEl.setAttribute("name", fieldDefinition.getName());
                fieldDefEl.setAttribute("value", fieldDefinition.getValueExpression());
                fieldDefEl.setAttribute("source", fieldDefinition.getValueSource().toString().toLowerCase());
                addParams(fieldDefinition.getParams(), fieldDefEl);
            }
        }

        if (conf.getDocumentExtractDefinitions() != null) {
            for (DocumentExtractDefinition docExtrDef : conf.getDocumentExtractDefinitions()) {
                Element docExtrEl = document.createElement("extract");
                indexerEl.appendChild(docExtrEl);
                if (docExtrDef.getPrefix() != null) docExtrEl.setAttribute("prefix", docExtrDef.getPrefix());
                docExtrEl.setAttribute("value", docExtrDef.getValueExpression());
                if (docExtrDef.getValueSource() != null) docExtrEl.setAttribute("source",
                        docExtrDef.getValueSource().toString().toLowerCase());
                if (docExtrDef.getMimeType() != null) docExtrEl.setAttribute("type", docExtrDef.getMimeType());
                addParams(docExtrDef.getParams(), docExtrEl);
            }
        }

        conf.setGlobalParams(params);

        TransformerFactory transformerFactory = TransformerFactory.newInstance();
        Transformer transformer =  transformerFactory.newTransformer();
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", String.valueOf(4));
        transformer.transform(new DOMSource(document), new StreamResult(os));

    }

    private static void addParams(Map<String,String> params, Element element) {
        if (params == null) return;

        for (Map.Entry<String,String> entry : params.entrySet()) {
            Element paramEl = element.getOwnerDocument().createElement("param");
            element.appendChild(paramEl);
            paramEl.setAttribute("name", entry.getKey());
            paramEl.setAttribute("value", entry.getValue());
        }
    }

}
