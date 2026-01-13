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

package org.apache.iotdb.pipe.api.customizer.parameter;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class PipeParametersTest {

  @Test
  public void keyReducerTest() {
    final PipeParameters parameters = new PipeParameters(new HashMap<>());
    parameters.addAttribute("sink.opcua.with-quality", "false");

    Assert.assertEquals(false, parameters.getBoolean("with-quality"));
    Assert.assertEquals(false, parameters.getBoolean("opcua.with-quality"));

    // Invalid
    parameters.addAttribute("sink.source.opcua.value-name", "false");
    parameters.addAttribute("opcua.sink.value-name", "false");
    Assert.assertNull(parameters.getString("value-name"));
  }
}
