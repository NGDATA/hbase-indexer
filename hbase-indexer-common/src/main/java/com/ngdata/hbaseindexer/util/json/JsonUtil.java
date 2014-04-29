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
package com.ngdata.hbaseindexer.util.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

public class JsonUtil {
    public static JsonNode getNode(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        return node.get(prop);
    }

    public static ArrayNode getArray(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isArray()) {
            throw new JsonFormatException("Not an array property: " + prop);
        }
        return (ArrayNode) node.get(prop);
    }

    public static ArrayNode getArray(JsonNode node, String prop, ArrayNode defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isArray()) {
            throw new JsonFormatException("Not an array property: " + prop);
        }
        return (ArrayNode) node.get(prop);
    }

    public static ObjectNode getObject(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isObject()) {
            throw new JsonFormatException("Not an object property: " + prop);
        }
        return (ObjectNode) node.get(prop);
    }

    public static ObjectNode getObject(JsonNode node, String prop, ObjectNode defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isObject()) {
            throw new JsonFormatException("Not an object property: " + prop);
        }
        return (ObjectNode) node.get(prop);
    }

    public static String getString(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isTextual()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    public static String getString(JsonNode node, String prop, String defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isTextual()) {
            throw new JsonFormatException("Not a string property: " + prop);
        }
        return node.get(prop).getTextValue();
    }

    public static Boolean getBoolean(JsonNode node, String prop, boolean defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (node.get(prop).isNull()) {
            return null;
        }
        if (!node.get(prop).isBoolean()) {
            throw new JsonFormatException("Not a boolean property: " + prop);
        }
        return node.get(prop).getBooleanValue();
    }

    public static Boolean getBoolean(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (node.get(prop).isNull()) {
            return null;
        }
        if (!node.get(prop).isBoolean()) {
            throw new JsonFormatException("Not a boolean property: " + prop);
        }
        return node.get(prop).getBooleanValue();
    }

    public static int getInt(JsonNode node, String prop, int defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isInt()) {
            throw new JsonFormatException("Not an integer property: " + prop);
        }
        return node.get(prop).getIntValue();
    }

    public static int getInt(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isInt()) {
            throw new JsonFormatException("Not an integer property: " + prop);
        }
        return node.get(prop).getIntValue();
    }

    public static long getLong(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        if (!node.get(prop).isLong() && !node.get(prop).isInt()) {
            throw new JsonFormatException("Not an long property: " + prop);
        }
        return node.get(prop).getLongValue();
    }

    public static Long getLong(JsonNode node, String prop, Long defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isLong() && !node.get(prop).isInt()) {
            throw new JsonFormatException("Not an long property: " + prop);
        }
        return node.get(prop).getLongValue();
    }

    public static Double getDouble(JsonNode node, String prop, Double defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        if (!node.get(prop).isLong() && !node.get(prop).isDouble()) {
            throw new JsonFormatException("Not a double property: " + prop);
        }
        return node.get(prop).getDoubleValue();
    }

    public static byte[] getBinary(JsonNode node, String prop) throws JsonFormatException {
        if (node.get(prop) == null) {
            throw new JsonFormatException("Missing required property: " + prop);
        }
        try {
            return node.get(prop).getBinaryValue();
        } catch (IOException e) {
            throw new JsonFormatException("Error reading binary data in property " + prop, e);
        }
    }

    public static byte[] getBinary(JsonNode node, String prop, byte[] defaultValue) throws JsonFormatException {
        if (node.get(prop) == null) {
            return defaultValue;
        }
        try {
            return node.get(prop).getBinaryValue();
        } catch (IOException e) {
            throw new JsonFormatException("Error reading binary data in property " + prop, e);
        }
    }

    public static List<String> getStrings(JsonNode node, String prop, List<String> defaultValue) throws JsonFormatException {
        ArrayNode arrayNode = getArray(node, prop, null);
        if (arrayNode == null) {
            return defaultValue;
        }
        List<String> elements = new ArrayList<String>();
        Iterator<JsonNode> elementItr = arrayNode.getElements();
        while (elementItr.hasNext()) {
            elements.add(elementItr.next().getValueAsText());
        }
        return elements;
    }
}
