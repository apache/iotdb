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

package org.apache.iotdb.db.pipe.pattern;

import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixTreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.UnionIoTDBTreePattern;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TreePatternPruningTest {

  @Test
  public void testUnionInternalPruning_Cover() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.db.d1.*,root.db.d1.s1");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue("Should be IoTDBTreePattern", result instanceof IoTDBTreePattern);
    Assert.assertEquals("root.db.d1.*", result.getPattern());
  }

  @Test
  public void testUnionInternalPruning_Duplicates() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.db.d1,root.db.d1");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof IoTDBTreePattern);
    Assert.assertEquals("root.db.d1", result.getPattern());
  }

  @Test
  public void testInclusionPrunedByExclusion_Partial() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.sg.d1,root.sg.d2");
                put(PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY, "root.sg.d1");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof IoTDBTreePattern);
    Assert.assertEquals("root.sg.d2", result.getPattern());
  }

  @Test
  public void testInclusionPrunedByExclusion_FullCoverage_Exception() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.sg.d1");
                put(PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY, "root.sg.**");
              }
            });

    try {
      TreePattern.parsePipePatternFromSourceParameters(params);
      Assert.fail("Should throw PipeException because Exclusion fully covers Inclusion");
    } catch (final PipeException ignored) {
      // Expected exception
    }
  }

  @Test
  public void testComplexPruning() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.sg.A,root.sg.B,root.sg.A.sub");
                put(PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY, "root.sg.A,root.sg.A.**");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof IoTDBTreePattern);
    Assert.assertEquals("root.sg.B", result.getPattern());
  }

  @Test
  public void testComplexPruning_Prefix() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATTERN_KEY, "root.sg.A,root.sg.B,root.sg.A.sub");
                put(PipeSourceConstant.SOURCE_PATTERN_EXCLUSION_KEY, "root.sg.A");
                put(PipeSourceConstant.SOURCE_PATTERN_FORMAT_KEY, "prefix");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof PrefixTreePattern);
    Assert.assertEquals("root.sg.B", result.getPattern());
  }

  @Test
  public void testUnionPreservedWhenNotCovered() {
    final PipeParameters params =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(PipeSourceConstant.SOURCE_PATH_KEY, "root.sg.d1,root.sg.d2");
                put(PipeSourceConstant.SOURCE_PATH_EXCLUSION_KEY, "root.other");
              }
            });

    final TreePattern result = TreePattern.parsePipePatternFromSourceParameters(params);

    Assert.assertTrue(result instanceof UnionIoTDBTreePattern);
    Assert.assertEquals("root.sg.d1,root.sg.d2", result.getPattern());
  }
}
