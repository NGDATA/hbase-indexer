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

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.Configurable;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.node.ObjectNode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class CsvByteArrayValueMapper implements ByteArrayValueMapper, Configurable {
    private List<String> defaultEntries;

    @Override
    public void configure(Map<String, String> config) {
        Map<String, String> params = config;
        if (params.containsKey("defaults")) {
            defaultEntries = Arrays.asList(params.get("defaults").split(","));
        } else {
            defaultEntries = Lists.newArrayList();
        }
    }

    @Override
    public Collection<? extends Object> map(byte[] input) {
        List<String> result = Lists.newArrayList(Arrays.asList(Bytes.toString(input).split(",")));
        result.addAll(defaultEntries);
        return result;
    }
}
