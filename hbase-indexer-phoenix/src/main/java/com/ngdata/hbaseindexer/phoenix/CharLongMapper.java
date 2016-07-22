/*
 * Copyright 2014 NGDATA nv
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ngdata.hbaseindexer.phoenix;

import com.google.common.collect.ImmutableList;
import com.ngdata.hbaseindexer.parse.ByteArrayValueMapper;
import org.apache.phoenix.schema.PDataType;

import java.util.Collection;

/**
 * Formats a Phoenix row key consisting of a CHAR(1) and a BIGINT into the string concatenation of these
 * two.
 *
 * <p>The use-case are the multitenant Lily keys, where the tenant is a CHAR(1).</p>
 */
public class CharLongMapper implements ByteArrayValueMapper {
    @Override
    public Collection<? extends Object> map(byte[] bytes) {
        PDataType longDataType = PDataType.LONG;
        PDataType charDataType = PDataType.CHAR;
        int charLength = 1;

        Object longValue = longDataType.toObject(bytes, bytes.length - longDataType.getByteSize(), longDataType.getByteSize());
        Object charValue = charDataType.toObject(bytes, bytes.length - longDataType.getByteSize() - charLength, charLength);

        return ImmutableList.of(charValue.toString() + longValue.toString());
    }
}
