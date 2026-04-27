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

package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.plan.execution.config.executor.ClusterConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe.CreatePipePluginStatement;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.fail;

public class ClusterConfigTaskExecutorTest {

  @Test
  public void testPipePlugin() {
    try {
      ClusterConfigTaskExecutor.getInstance()
          .createPipePlugin(
              new CreatePipePluginStatement("TestProcessor", false, "someClass", "uri"))
          .get();
      Assert.fail();
    } catch (final Exception e) {
      Assert.assertTrue(
          e.getMessage()
              .contains("The scheme of URI is not set, please specify the scheme of URI."));
    }

    try {
      ClusterConfigTaskExecutor.getInstance()
          .createPipePlugin(
              new CreatePipePluginStatement("TestProcessor", false, "someClass", "file:.*"))
          .get();
      Assert.fail();
    } catch (final Exception e) {
      Assert.assertTrue(e.getMessage().contains("URI is not hierarchical"));
    }

    try {
      ClusterConfigTaskExecutor.getInstance()
          .createPipePlugin(
              new CreatePipePluginStatement(
                  "TestProcessor",
                  false,
                  "org.apache.iotdb.db.pipe.example.TestProcessor",
                  new File(
                              System.getProperty("user.dir")
                                  + File.separator
                                  + "target"
                                  + File.separator
                                  + "test-classes"
                                  + File.separator)
                          .toURI()
                      + "PipePlugin.jar"))
          .get();
      fail();
    } catch (final Exception e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Failed to get executable for PipePlugin TestProcessor, please check the URI."));
    }
  }
}
