/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.conf;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URL;

public class ConfigNodeDescriptorTest {

  private final String confPath = System.getProperty(ConfigNodeConf.CONF_NAME, null);

  @Before
  public void init() {
    org.apache.catalina.webresources.TomcatURLStreamHandlerFactory.getInstance();
  }

  @After
  public void clear() {
    if (confPath != null) {
      System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, confPath);
    } else {
      System.clearProperty(ConfigNodeConstant.CONFIG_NODE_CONF);
    }
  }

  @Test
  public void testConfigURLWithFileProtocol() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();
    String pathString = "file:/usr/local/bin";

    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, pathString);
    URL confURL = desc.getPropsUrl();
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithClasspathProtocol() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();

    String pathString = "classpath:/root/path";
    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, pathString);
    URL confURL = desc.getPropsUrl();
    Assert.assertTrue(confURL.toString().startsWith(pathString));
  }

  @Test
  public void testConfigURLWithPlainFilePath() {
    ConfigNodeDescriptor desc = ConfigNodeDescriptor.getInstance();
    URL path = ConfigNodeConf.class.getResource("/" + ConfigNodeConf.CONF_NAME);
    // filePath is a plain path string
    String filePath = path.getFile();
    System.setProperty(ConfigNodeConstant.CONFIG_NODE_CONF, filePath);
    URL confURL = desc.getPropsUrl();
    Assert.assertEquals(confURL.toString(), path.toString());
  }
}
