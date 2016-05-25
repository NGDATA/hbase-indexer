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

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

class MRTestUtil {
    
    
    private HBaseTestingUtility hbaseTestUtil;
    
    public MRTestUtil(HBaseTestingUtility hbaseTestUtil) {
        this.hbaseTestUtil = hbaseTestUtil;
    }
    
    public void runTool(String... args) throws Exception {
        HBaseMapReduceIndexerTool tool = new HBaseMapReduceIndexerTool();
        int exitCode = ToolRunner.run(
                hbaseTestUtil.getConfiguration(),
                tool,
                args);
        assertEquals(0, exitCode);
    }
    
    public void startMrCluster() throws Exception {
        MiniMRCluster mrCluster = hbaseTestUtil.startMiniMapReduceCluster();
        Configuration conf = hbaseTestUtil.getConfiguration();
        conf.set("mapred.job.tracker", mrCluster.createJobConf().get("mapred.job.tracker"));
    }
    
    public static File substituteZkHost(File file, String zkConnectString) throws IOException {
      String str = Files.toString(file, Charsets.UTF_8);
      str = str.replace("_MYPATTERN_", zkConnectString);
      File tmp = File.createTempFile("tmpIndexer", ".xml");
      tmp.deleteOnExit();
      Files.write(str, tmp, Charsets.UTF_8);
      return tmp;
    }

}
