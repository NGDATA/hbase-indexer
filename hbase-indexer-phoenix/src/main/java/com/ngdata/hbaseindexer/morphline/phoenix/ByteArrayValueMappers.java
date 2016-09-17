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
package com.ngdata.hbaseindexer.morphline.phoenix;

import java.lang.reflect.Array;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.PhoenixArray;
import org.apache.phoenix.schema.SortOrder;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;

/**
 * Contains factory methods for {@link ByteArrayValueMapper}s.
 */
class ByteArrayValueMappers {

	private static Log log = LogFactory.getLog(ByteArrayValueMappers.class);

	/**
	 * Get a {@link ByteArrayValueMapper} for a given type.
	 * 
	 * @param type
	 *            the mapper type
	 * @param sortOrder
	 *            sort order
	 * @return the requested mapper
	 */
	static ByteArrayValueMapper getMapper(PDataType type, SortOrder sortOrder, int offset, int length) {
		Preconditions.checkNotNull(type, "Phoenix type is required");
		if (sortOrder == null) {
			sortOrder = SortOrder.getDefault();
		}
		Preconditions.checkArgument(offset <= 0 || length > 0,
				"If offset is positive number then length must be a positive number too.");
		if (type.isArrayType()) {
			return new ArrayValueMapper(type, sortOrder, offset, length);
		} else {
			return new SingleValueMapper(type, sortOrder, offset, length);
		}
	}

	private static abstract class AbstractByteValueMapper implements ByteArrayValueMapper {

		protected final PDataType type;
		protected final SortOrder sortOrder;
		protected final int offset;
		protected final int length;

		protected AbstractByteValueMapper(PDataType type, SortOrder sortOrder, int offset, int length) {
			this.type = type;
			this.sortOrder = sortOrder;
			this.offset = offset;
			this.length = length;
		}

		@Override
		public final Collection<Object> map(byte[] input) {
			try {
				return mapInternal(input);
			} catch (IllegalArgumentException e) {
				log.warn(String.format("Error mapping byte value %s to %s", Bytes.toStringBinary(input), type.name()),
						e);
				return ImmutableList.of();
			}
		}

		protected abstract Collection<Object> mapInternal(byte[] input);
	}

	private static class ArrayValueMapper extends AbstractByteValueMapper {

		protected ArrayValueMapper(PDataType type, SortOrder sortOrder, int offset, int length) {
			super(type, sortOrder, offset, length);
		}

		@Override
		protected Collection<Object> mapInternal(byte[] input) {
			Object array;
			try {
				if (offset > 0) {
					array = ((PhoenixArray) type.toObject(input, offset, length, type, sortOrder)).getArray();
				} else {
					array = ((PhoenixArray) type.toObject(input, sortOrder)).getArray();
				}
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}

			if (array.getClass().isArray()) {
				Collection<Object> result = new ArrayList<Object>();
				int arrayLength = Array.getLength(array);
				for (int i = 0; i < arrayLength; i++) {
					result.add(Array.get(array, i));
				}
				return result;
			}
			throw new IllegalStateException();
		}
	}

	private static class SingleValueMapper extends AbstractByteValueMapper {

		protected SingleValueMapper(PDataType type, SortOrder sortOrder, int offset, int length) {
			super(type, sortOrder, offset, length);
		}

		@Override
		protected Collection<Object> mapInternal(byte[] input) {
			if (offset > 0) {
				return ImmutableList.of(type.toObject(input, offset, length, type, sortOrder));
			} else {
				return ImmutableList.of(type.toObject(input, sortOrder));
			}
		}
	}
}
