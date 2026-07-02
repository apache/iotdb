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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PipeParametersTest {

  @Test
  public void keyReducerTest() {
    final PipeParameters parameters = new PipeParameters(new HashMap<>());
    parameters.addAttribute("sink.opcua.with-quality", "true");

    Assert.assertEquals(true, parameters.getBoolean("with-quality"));
    Assert.assertEquals(true, parameters.getBoolean("opcua.with-quality"));
    Assert.assertTrue(parameters.hasAnyAttributes("missing-key", "sink.opcua.with-quality"));

    // Invalid
    parameters.addAttribute("sink.source.opcua.value-name", "false");
    parameters.addAttribute("opcua.sink.value-name", "false");
    Assert.assertNull(parameters.getString("value-name"));
  }

  @Test
  public void equivalentAttributeReplacementTest() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("sink.opcua.with-quality", "false");
    final PipeParameters parameters = new PipeParameters(attributes);

    final Map<String, String> replacements = new HashMap<>();
    replacements.put("connector.opcua.with-quality", "true");
    parameters.addOrReplaceEquivalentAttributes(new PipeParameters(replacements));

    Assert.assertFalse(parameters.getAttribute().containsKey("sink.opcua.with-quality"));
    Assert.assertEquals("true", parameters.getAttribute().get("connector.opcua.with-quality"));
  }

  @Test
  public void equivalentAttributeReplacementWithCloneTest() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("sink.opcua.with-quality", "false");
    final PipeParameters parameters = new PipeParameters(attributes);

    final Map<String, String> replacements = new HashMap<>();
    replacements.put("connector.opcua.with-quality", "true");
    final PipeParameters clonedParameters =
        parameters.addOrReplaceEquivalentAttributesWithClone(new PipeParameters(replacements));

    Assert.assertEquals("false", parameters.getAttribute().get("sink.opcua.with-quality"));
    Assert.assertEquals("true", clonedParameters.getString("with-quality"));
    Assert.assertEquals(
        "true",
        clonedParameters.getStringOrDefault(
            Arrays.asList("missing-key", "sink.opcua.with-quality"), "default"));
  }

  @Test
  public void toStringShouldHideSensitiveValues() {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put("sink.password", "secret");
    attributes.put("connector.ssl.trust-store-pwd", "credential");
    attributes.put("visible", "value");

    final String parametersString = new PipeParameters(attributes).toString();

    Assert.assertTrue(parametersString.contains("sink.password=******"));
    Assert.assertTrue(parametersString.contains("connector.ssl.trust-store-pwd=******"));
    Assert.assertTrue(parametersString.contains("visible=value"));
    Assert.assertFalse(parametersString.contains("secret"));
    Assert.assertFalse(parametersString.contains("credential"));
  }
}
