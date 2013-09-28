/*
 * Copyright 2012 NGDATA nv
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
package com.ngdata.sep.impl;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.zookeeper.EmptyWatcher;
import org.apache.hadoop.hbase.regionserver.wal.HLogUtil;

import java.util.ArrayList;
import java.util.List;

public class HBaseShims {
  public static Get newGet() { return new Get(" ".getBytes()); }
  public static Result newResult(List<KeyValue> list) { return Result.create(new ArrayList<Cell>(list)); }
  public static EmptyWatcher getEmptyWatcherInstance() { return EmptyWatcher.instance; }
  public static String getHLogDirectoryName(String serverName) { return HLogUtil.getHLogDirectoryName(serverName); }
}
