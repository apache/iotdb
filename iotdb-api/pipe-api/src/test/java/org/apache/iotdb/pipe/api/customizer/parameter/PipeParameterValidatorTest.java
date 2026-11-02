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

import org.apache.iotdb.pipe.api.exception.PipeAttributeNotProvidedException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PipeParameterValidatorTest {

  @Test
  public void testDatabaseAliasesMatchOneAttribute() {
    for (final String key : Arrays.asList("connector.database", "sink.database", "database")) {
      final PipeParameterValidator validator =
          new PipeParameterValidator(
              new PipeParameters(Collections.singletonMap(key, "root.target")));
      for (final boolean required : new boolean[] {false, true}) {
        Assert.assertSame(
            validator,
            validator.validateSynonymAttributes(
                Collections.singletonList("connector.database"),
                Collections.singletonList("sink.database"),
                required));
      }
    }
  }

  @Test
  public void testDatabaseAliasesRejectDistinctAttributes() {
    final String[] keys = {"connector.database", "sink.database", "database"};
    for (int i = 0; i < keys.length; i++) {
      for (int j = i + 1; j < keys.length; j++) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(keys[i], "root.target");
        attributes.put(keys[j], "root.target");
        Assert.assertThrows(
            PipeParameterNotValidException.class,
            () ->
                new PipeParameterValidator(new PipeParameters(attributes))
                    .validateSynonymAttributes(
                        Collections.singletonList("connector.database"),
                        Collections.singletonList("sink.database"),
                        false));
      }
    }
  }

  @Test
  public void testDistinctSynonymsRemainMutuallyExclusive() {
    for (final String userKey : Arrays.asList("connector.user", "sink.user", "user")) {
      for (final String usernameKey :
          Arrays.asList("connector.username", "sink.username", "username")) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put(userKey, "user");
        attributes.put(usernameKey, "user");
        Assert.assertThrows(
            PipeParameterNotValidException.class,
            () ->
                new PipeParameterValidator(new PipeParameters(attributes))
                    .validateSynonymAttributes(
                        Arrays.asList("connector.user", "sink.user"),
                        Arrays.asList("connector.username", "sink.username"),
                        false));
      }
    }
  }

  @Test
  public void testShallowPrefixesMatchOneAttribute() {
    new PipeParameterValidator(
            new PipeParameters(Collections.singletonMap("opcua.database", "root.target")))
        .validateSynonymAttributes(
            Collections.singletonList("connector.opcua.database"),
            Collections.singletonList("sink.opcua.database"),
            true);
  }

  @Test
  public void testRequiredSynonymsSupportUnprefixedAttributes() {
    for (final String key : Arrays.asList("output.series", "output-series")) {
      new PipeParameterValidator(new PipeParameters(Collections.singletonMap(key, "root.db.s")))
          .validateSynonymAttributes(
              Collections.singletonList("processor.output.series"),
              Collections.singletonList("processor.output-series"),
              true);
    }

    final PipeParameterValidator validator =
        new PipeParameterValidator(new PipeParameters(Collections.emptyMap()));
    validator.validateSynonymAttributes(
        Collections.singletonList("connector.database"),
        Collections.singletonList("sink.database"),
        false);
    Assert.assertThrows(
        PipeAttributeNotProvidedException.class,
        () ->
            validator.validateSynonymAttributes(
                Collections.singletonList("connector.database"),
                Collections.singletonList("sink.database"),
                true));
  }
}
