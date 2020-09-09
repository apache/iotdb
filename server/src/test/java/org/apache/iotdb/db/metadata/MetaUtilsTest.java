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
package org.apache.iotdb.db.metadata;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.junit.Assert;
import org.junit.Test;

public class MetaUtilsTest {

  @Test
  public void testSplitPathToNodes() throws IllegalPathException {
    assertEquals(Arrays.asList("root", "sg", "d1", "s1").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.s1"));

    assertEquals(Arrays.asList("root", "sg", "d1", "\"s.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.\"s.1\""));

    assertEquals(Arrays.asList("root", "sg", "d1", "\"s\\\".1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.\"s\\\".1\""));

    assertEquals(Arrays.asList("root", "\"s g\"", "d1", "\"s.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.\"s g\".d1.\"s.1\""));

    assertEquals(Arrays.asList("root", "\"s g\"", "\"d_.1\"", "\"s.1.1\"").toArray(),
        MetaUtils.splitPathToDetachedPath("root.\"s g\".\"d_.1\".\"s.1.1\""));

    assertEquals(Arrays.asList("root", "1").toArray(), MetaUtils.splitPathToDetachedPath("root.1"));

    assertEquals(Arrays.asList("root", "sg", "d1", "s", "1").toArray(),
        MetaUtils.splitPathToDetachedPath("root.sg.d1.s.1"));

    try {
      MetaUtils.splitPathToDetachedPath("root.sg.\"d.1\"\"s.1\"");
    } catch (IllegalPathException e) {
      Assert.assertTrue(e.getMessage().contains("Illegal path: "));
    }

    try {
      MetaUtils.splitPathToDetachedPath("root.sg.d1.\'s1\'");
    } catch (IllegalPathException e) {
      Assert.assertTrue(e.getMessage().contains("Illegal path with single quote: "));
    }
  }

}