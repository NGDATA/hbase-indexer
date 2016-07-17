package com.ngdata.hbaseindexer.parse.extract;

import java.util.Collection;
import java.util.Collections;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

import com.ngdata.hbaseindexer.parse.ByteArrayExtractor;

public class RowkeyExtractor implements ByteArrayExtractor {

	@Override
	public boolean containsTarget(Result result) {
		return true;
	}

	@Override
	public Collection<byte[]> extract(Result result) {
		return Collections.singletonList(result.getRow());
	}

	@Override
	public byte[] getColumnFamily() {
		return null;
	}

	@Override
	public byte[] getColumnQualifier() {
		return null;
	}

	@Override
	public boolean isApplicable(KeyValue keyValue) {
		return true;
	}

}
