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

package org.apache.iotdb.db.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SchemaPathUtilsTest {

  @Test
  public void testDisplayDurationLikeNodeWithoutBackQuotes() {
    assertEquals(
        "root.sg.d1.123d", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.123d"));
    assertEquals(
        "root.sg.d1.123h", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.123h"));
    assertEquals(
        "root.sg.d1.123w", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.123w"));
  }

  @Test
  public void testDisplayNumericNodeWithBackQuotes() {
    assertEquals(
        "root.sg.d1.`001`", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.001"));
    assertEquals(
        "root.sg.d1.`1e3`", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.1e3"));
    assertEquals(
        "root.sg.d1.`0`", SchemaPathUtils.getFullPathWithNecessaryBackQuotes("root.sg.d1.0"));
  }
}
