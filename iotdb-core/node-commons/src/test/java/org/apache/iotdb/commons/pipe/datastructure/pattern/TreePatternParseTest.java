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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TreePatternParseTest {

  @Test
  public void testPatternAndPatternInclusionPreserved() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATTERN_KEY, "root.sg.A");
                put(PipeSourceConstant.SOURCE_PATTERN_INCLUSION_KEY, "root.sg.B");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof UnionTreePattern);
    Assert.assertEquals("root.sg.A,root.sg.B", result.getPattern());
  }

  @Test
  public void testPathAndPathInclusionPreserved() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.sg.d1");
                put(PipeSourceConstant.SOURCE_PATH_INCLUSION_KEY, "root.sg.d2,root.sg.d3");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof UnionIoTDBTreePattern);
    Assert.assertEquals("root.sg.d1,root.sg.d2,root.sg.d3", result.getPattern());
  }

  @Test
  public void testPathInclusionWithPathExclusionPreserved() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_INCLUSION_KEY, "root.sg.**");
                put(PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY, "root.sg.d1,root.sg.d2");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof WithExclusionIoTDBTreePattern);
    Assert.assertEquals(
        "INCLUSION(root.sg.**), EXCLUSION(root.sg.d1,root.sg.d2)", result.getPattern());
  }
}
