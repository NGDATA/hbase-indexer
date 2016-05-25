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
package com.ngdata.hbaseindexer.mr;

import com.ngdata.hbaseindexer.parse.ResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Fake ResultToSolrMapper implementation to allow specifying a Get.
 */
public class MockResultToSolrMapper implements ResultToSolrMapper {

    @Override
    public boolean isRelevantKV(KeyValue kv) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Get getGet(byte[] row) {
        Get get = new Get(Bytes.toBytes(" "));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("firstname"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("lastname"));
        get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        return get;
    }

    @Override
    public boolean containsRequiredData(Result result) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void map(Result result, SolrUpdateWriter updateWriter) {
        throw new UnsupportedOperationException("Not implemented");
    }

}
