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
package com.ngdata.hbaseindexer.util.solr;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.solr.cloud.ZkController;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.xml.sax.SAXException;

/**
 * Loads a SolrConfig (and other configuration elements) from a SolrCloud ZooKeeper.
 */
public class SolrConfigLoader extends SolrResourceLoader {

    private ZooKeeper zk;
    private String configZkPath;

    /**
     * Instantiate with the name of the configuration directory and the ZooKeeper used to access it.
     */
    public SolrConfigLoader(String collection, ZooKeeper zooKeeper) {
        super("solr");
        this.zk = zooKeeper;
        this.configZkPath = getCollectionConfigPath(collection);
    }
    
    private String getCollectionConfigPath(String collectionName) {
        try {
            byte[] data = zk.getData("/collections/" + collectionName, false, null);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonParser jsonParser = objectMapper.getJsonFactory().createJsonParser(data);
            JsonNode collectionNode = objectMapper.readTree(jsonParser);
            return ZkController.CONFIGS_ZKNODE + "/" + collectionNode.get(ZkController.CONFIGNAME_PROP).asText();
        } catch (Exception e) {
            // TODO Better exception handling here
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Load a {@code SolrConfig} from ZooKeeper.
     * <p>
     * The returned SolrConfig will include this instance as its {@code SolrResourceLoader}.
     */
    public SolrConfig loadSolrConfig() throws ParserConfigurationException, IOException, SAXException {
        return new SolrConfig(this, SolrConfig.DEFAULT_CONF_FILE, null);
    }
    
    // Overrides of SolrResourceLoader are all taken from org.apache.solr.cloud.ZkSolrResourceLoader
    // The ZkSolrResourceLoader isn't used directly because it relies on a ZkController, which is much
    // more extensive than what we need here
    
    /**
     * Opens any resource by its name. By default, this will look in multiple
     * locations to load the resource: $configDir/$resource from ZooKeeper.
     * It will look for it in any jar
     * accessible through the class loader if it cannot be found in ZooKeeper.
     * Override this method to customize loading resources.
     * 
     * @return the stream for the named resource
     */
    @Override
    public InputStream openResource(String resource) throws IOException {
      InputStream is = null;
      String file = configZkPath + "/" + resource;
      try {
        if (zk.exists(file, false) != null) {
          byte[] bytes = zk.getData(configZkPath + "/" + resource, false, null);
          return new ByteArrayInputStream(bytes);
        }
      } catch (Exception e) {
        throw new IOException("Error opening " + file, e);
      }
      try {
        // delegate to the class loader (looking into $INSTANCE_DIR/lib jars)
        is = classLoader.getResourceAsStream(resource);
      } catch (Exception e) {
        throw new IOException("Error opening " + resource, e);
      }
      if (is == null) {
        throw new IOException("Can't find resource '" + resource
            + "' in classpath or '" + configZkPath + "', cwd="
            + System.getProperty("user.dir"));
      }
      return is;
    }

    @Override
    public String getConfigDir() {
      throw new ZooKeeperException(
          ErrorCode.SERVER_ERROR,
          "ZkSolrResourceLoader does not support getConfigDir() - likely, what you are trying to do is not supported in ZooKeeper mode");
    }
    
    @Override
    public String[] listConfigDir() {
      List<String> list;
      try {
        list = zk.getChildren(configZkPath, false, null);
      } catch (InterruptedException e) {
        // Restore the interrupted status
        Thread.currentThread().interrupt();
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      } catch (KeeperException e) {
        log.error("", e);
        throw new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR,
            "", e);
      }
      return list.toArray(new String[0]);
    }
}
