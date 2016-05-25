/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MorphlineClasspathUtil {
  
  private static final AtomicBoolean IS_INITIALIZED = new AtomicBoolean(false);
  
  private static final Logger LOG = LoggerFactory.getLogger(MorphlineClasspathUtil.class);

  /**
   * Ensure scripting support for Java via morphline "java" command works even in dryRun mode,
   * i.e. when executed in the client side driver JVM. To do so, collect all classpath URLs from
   * the class loaders chain that org.apache.hadoop.util.RunJar (hadoop jar xyz-job.jar) and
   * org.apache.hadoop.util.GenericOptionsParser (--libjars) have installed, then tell
   * FastJavaScriptEngine.parse() where to find classes that JavaBuilder scripts might depend on.
   * This ensures that scripts that reference external java classes compile without exceptions
   * like this:
   * 
   * ... caused by compilation failed: mfm:///MyJavaClass1.java:2: package
   * org.kitesdk.morphline.api does not exist
   */
  public static void setupJavaCompilerClasspath() {
    if (!IS_INITIALIZED.compareAndSet(false, true)) {
      return; // update java.class.path system property at most once to keep Eclipse JUnit happy
    }
    
    LOG.trace("morphline java.class.path: {}", System.getProperty("java.class.path"));
    String fullClassPath = "";
    ClassLoader loader = Thread.currentThread().getContextClassLoader(); // see org.apache.hadoop.util.RunJar
    while (loader != null) { // walk class loaders, collect all classpath URLs
      if (loader instanceof URLClassLoader) {
        URL[] classPathPartURLs = ((URLClassLoader) loader).getURLs(); // see org.apache.hadoop.util.RunJar
        LOG.trace("morphline classPathPartURLs: {}", Arrays.asList(classPathPartURLs));
        StringBuilder classPathParts = new StringBuilder();
        for (URL url : classPathPartURLs) {
          File file;
          try {
            file = new File(url.toURI());
          } catch (URISyntaxException e) {
            throw new RuntimeException(e); // unreachable
          }
          if (classPathPartURLs.length > 0) {
            classPathParts.append(File.pathSeparator);
          }
          classPathParts.append(file.getPath());
        }
        LOG.trace("morphline classPathParts: {}", classPathParts);
        String separator = File.pathSeparator;
        if (fullClassPath.length() == 0 || classPathParts.length() == 0) {
          separator = "";
        }
        fullClassPath = classPathParts + separator + fullClassPath;
      }
      loader = loader.getParent();
    }
    
    // tell FastJavaScriptEngine.parse() where to find the classes that the script might depend on
    if (fullClassPath.length() > 0) {
      assert System.getProperty("java.class.path") != null;
      fullClassPath = System.getProperty("java.class.path") + File.pathSeparator + fullClassPath;
      LOG.trace("morphline fullClassPath: {}", fullClassPath);
      System.setProperty("java.class.path", fullClassPath); // see FastJavaScriptEngine.parse()
    }
  }

}
