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
package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.exception.IllegalPathException;

import org.junit.Assert;
import org.junit.Test;

public class PartialPathTest {

  @Test
  public void testOverlapWith() throws IllegalPathException {
    PartialPath[][] pathPairs =
        new PartialPath[][] {
          new PartialPath[] {new PartialPath("root.**"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.**.*"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.**.s"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.*.**"), new PartialPath("root.sg.**")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.**.s")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.sg.*.s")},
          new PartialPath[] {new PartialPath("root.*.d.s"), new PartialPath("root.sg.d2.s")}
        };
    boolean[] results = new boolean[] {true, true, true, true, true, true, false};
    for (int i = 0; i < pathPairs.length; i++) {
      Assert.assertEquals(results[i], pathPairs[i][0].overlapWith(pathPairs[i][1]));
    }
  }
}
