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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineCompilationException;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.Configs;
import org.kitesdk.morphline.base.Validator;

import com.google.common.collect.Lists;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import com.typesafe.config.Config;

public class ConvertPhoenixTypeBuilder implements CommandBuilder {

	@Override
	public Command build(Config config, Command parent, Command child, MorphlineContext context) {
		return new ConvertPhoenixType(this, config, parent, child, context);
	}

	@Override
	public Collection<String> getNames() {
		return Collections.singletonList("convertPhoenixType");
	}

	private static final class ConvertPhoenixType extends AbstractCommand {

		private final List<Field> fields = new ArrayList<Field>();

		public ConvertPhoenixType(CommandBuilder builder, Config config, Command parent, Command child,
				MorphlineContext context) {
			super(builder, config, parent, child, context);
			for (Config mapping : getConfigs().getConfigList(config, "fields")) {
				fields.add(new Field(mapping));
			}
			validateArguments();
		}

		@Override
		protected boolean doProcess(Record record) {
			for (Field field : fields) {
				@SuppressWarnings("unchecked")
				List<byte[]> list = Lists.newArrayList(record.get(field.field));
				record.removeAll(field.field);
				for (byte[] bs : list) {
					for (Object value : field.map(bs)) {
						record.put(field.field, value);
					}
				}
			}
			return super.doProcess(record);
		}
	}

	private static final class Field {
		private final String field;
		private final PDataType type;
		private final SortOrder sortOrder;
		private final int offset;
		private final int length;
		private final ByteArrayValueMapper mapper;

		public Field(Config config) {
			Configs configs = new Configs();
			this.field = configs.getString(config, "field");
			this.type = new Validator<PDataType>().validateEnum(config, configs.getString(config, "type"),
					PDataType.class);
			this.sortOrder = new Validator<SortOrder>().validateEnum(config, configs.getString(config, "sortOrder",
					SortOrder.getDefault().name()), SortOrder.class);
			this.offset = configs.getInt(config, "offset", 0);
			this.length = configs.getInt(config, "length", -1);
			if (offset > 0 && length <= 0) {
				throw new MorphlineCompilationException("If offset is set then length must be a positive number.",
						config);
			}
			this.mapper = ByteArrayValueMappers.getMapper(type, sortOrder, offset, length);
			configs.validateArguments(config);
		}

		public Collection<? extends Object> map(byte[] bytes) {
			return mapper.map(bytes);
		}
	}
}
