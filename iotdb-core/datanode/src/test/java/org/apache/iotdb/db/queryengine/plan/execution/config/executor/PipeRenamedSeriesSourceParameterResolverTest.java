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

package org.apache.iotdb.db.queryengine.plan.execution.config.executor;

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PipeRenamedSeriesSourceParameterResolverTest {

  @Test
  public void testHasWildcardIgnoresBackquotedWildcards() throws Exception {
    Assert.assertFalse(hasWildcard(null));
    Assert.assertFalse(hasWildcard(""));
    Assert.assertFalse(hasWildcard("root.db.d1.s1"));
    Assert.assertTrue(hasWildcard("root.db.*"));
    Assert.assertTrue(hasWildcard("root.db.**"));
    Assert.assertTrue(hasWildcard("root.*.d1.s1"));
    Assert.assertTrue(hasWildcard("root.db.d1.*"));
    Assert.assertTrue(hasWildcard("root.db.`d*`.s*"));

    Assert.assertFalse(hasWildcard("root.db.`d*`.s"));
    Assert.assertFalse(hasWildcard("root.db.`d**`.s"));
    Assert.assertFalse(hasWildcard("root.db.`d``*`.s"));
  }

  @Test
  public void testHasWildcardHandlesBackquoteBoundaries() throws Exception {
    Assert.assertTrue(hasWildcard("root.`db*`.d1.`s*`.*"));
    Assert.assertTrue(hasWildcard("root.`db``*`.d1.**"));
    Assert.assertTrue(hasWildcard("root.`db```.d1.s1*"));

    Assert.assertFalse(hasWildcard("root.`db*`.`d**`.`s*`"));
    Assert.assertFalse(hasWildcard("root.`db``*`.`d``**`.`s``*`"));
    Assert.assertFalse(hasWildcard("root.`db```.`d1`.`s1`"));
  }

  @Test
  public void testDefaultPrefixFormatFetchesExactAndDescendants() throws Exception {
    Assert.assertEquals(
        Arrays.asList("root.db.d1", "root.db.d1.**"), getSchemaFetchPatterns("root.db.d1", false));
    Assert.assertEquals(
        Collections.singletonList("root.db.d1.*"), getSchemaFetchPatterns("root.db.d1.*", false));
    Assert.assertEquals(
        Collections.singletonList("root.db.d1"), getSchemaFetchPatterns("root.db.d1", true));
  }

  private static boolean hasWildcard(final String pattern) throws Exception {
    final Method method =
        PipeRenamedSeriesSourceParameterResolver.class.getDeclaredMethod(
            "hasWildcard", String.class);
    method.setAccessible(true);
    return (boolean) method.invoke(null, pattern);
  }

  @SuppressWarnings("unchecked")
  private static List<String> getSchemaFetchPatterns(
      final String pattern, final boolean isIoTDBFormat) throws Exception {
    final Method method =
        PipeRenamedSeriesSourceParameterResolver.class.getDeclaredMethod(
            "getSchemaFetchPatterns", String.class, boolean.class);
    method.setAccessible(true);
    return (List<String>) method.invoke(null, pattern, isIoTDBFormat);
  }
}
