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

package org.apache.iotdb.confignode.pipe.annotation;

import org.apache.iotdb.commons.pipe.datastructure.result.Result;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityTestUtils;
import org.apache.iotdb.pipe.api.PipePlugin;

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;

import java.util.Set;

import static org.junit.Assert.fail;

public class PipePluginAnnotationTest {

  @Test
  public void testPipePluginVisibility() {
    // Use the Reflections library to scan the classpath
    final Reflections reflections =
        new Reflections("org.apache.iotdb.confignode", new SubTypesScanner(false));
    final Set<Class<? extends PipePlugin>> subTypes = reflections.getSubTypesOf(PipePlugin.class);
    final Result<Void, String> result =
        VisibilityTestUtils.testVisibilityCompatibilityEntry(subTypes);
    if (result.isErr()) {
      fail(result.getErr());
    }
  }
}
