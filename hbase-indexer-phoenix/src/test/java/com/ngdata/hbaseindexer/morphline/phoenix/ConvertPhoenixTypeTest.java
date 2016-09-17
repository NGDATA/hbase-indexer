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
package com.ngdata.hbaseindexer.morphline.phoenix;

import static com.ngdata.sep.impl.HBaseShims.newResult;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.Before;
import org.junit.Test;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.Record;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.ngdata.hbaseindexer.morphline.MorphlineResultToSolrMapper;
import com.ngdata.hbaseindexer.parse.SolrUpdateWriter;

public class ConvertPhoenixTypeTest {
	private static final byte[] ROW = Bytes.toBytes("row");
	private static final byte[] COLUMN_FAMILY_A = Bytes.toBytes("cfA");
	private static final byte[] COLUMN_FAMILY_B = Bytes.toBytes("cfB");
	private static final byte[] QUALIFIER_A = Bytes.toBytes("qualifierA");
	private static final byte[] PREFIX = Bytes.toBytes("prefix");
	private static final List<Integer> INTS = Arrays.asList(1, 10, 100, 1000, 10000);
	private static final List<Long> LONGS = Arrays.asList(1l, 10l, 100l, 1000l, 10000l);

	private SolrUpdateWriter updateWriter;
	private ArgumentCaptor<SolrInputDocument> solrInputDocCaptor;

	@Before
	public void setUp() {
		updateWriter = mock(SolrUpdateWriter.class);
		solrInputDocCaptor = ArgumentCaptor.forClass(SolrInputDocument.class);
	}

	@Test
	public void testConvertNumbers() throws Exception {
		MorphlineResultToSolrMapper resultMapper = new MorphlineResultToSolrMapper();
		resultMapper.configure(ImmutableMap.of(
				MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM,
				"src/test/resources/test-morphlines/convertPhoenixType.conf")
				);

		List<KeyValue> keyValues = new ArrayList<KeyValue>();
		for (Integer intValue : INTS) {
			ByteBuffer bb = ByteBuffer.allocate(PREFIX.length + Bytes.SIZEOF_INT);
			bb.put(PREFIX);
			bb.put(PDataType.INTEGER.toBytes(intValue));
			keyValues.add(new KeyValue(ROW, COLUMN_FAMILY_A, bb.array()));
		}
		PhoenixArray pLongs = new PhoenixArray(PDataType.LONG, LONGS.toArray());
		byte[] pLongsBytes = PDataType.LONG_ARRAY.toBytes(pLongs, SortOrder.DESC);
		keyValues.add(new KeyValue(ROW, COLUMN_FAMILY_B, QUALIFIER_A, pLongsBytes));

		Result result = newResult(keyValues);

		resultMapper.map(result, updateWriter);
		verify(updateWriter).add(solrInputDocCaptor.capture());

		SolrInputDocument solrDocument = solrInputDocCaptor.getValue();

		assertEquals(expectedMap(), toRecord(solrDocument).getFields());
	}

	@Test(expected = MorphlineCompilationException.class)
	public void testTypoValidation_fails() {
		MorphlineResultToSolrMapper resultMapper = new MorphlineResultToSolrMapper();
		resultMapper.configure(ImmutableMap.of(
				MorphlineResultToSolrMapper.MORPHLINE_FILE_PARAM,
				"src/test/resources/test-morphlines/convertPhoenixTypo.conf")
				);
	}

	private Multimap<String, Object> expectedMap() {
		Multimap<String, Object> expectedMap = ArrayListMultimap.create();
		for (Integer i : INTS) {
			expectedMap.put("fieldA", i);
		}
		for (Long l : LONGS) {
			expectedMap.put("fieldB", l);
		}
		return expectedMap;
	}

	private Record toRecord(SolrInputDocument doc) {
		Record record = new Record();
		for (Entry<String, SolrInputField> entry : doc.entrySet()) {
			record.getFields().putAll(entry.getKey(), entry.getValue().getValues());
		}
		return record;
	}

}
