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

package org.apache.iotdb.calc.utils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ObjectPathNamingTest {

  @Test
  public void parseLegacyAndVersionedNames() {
    assertEquals(123L, ObjectPathNaming.parseTime("123.bin"));
    assertEquals(ObjectPathNaming.LEGACY_VERSION, ObjectPathNaming.parseVersion("123.bin"));
    assertTrue(ObjectPathNaming.isLegacyBin("123.bin"));

    assertEquals(123L, ObjectPathNaming.parseTime("123_7.bin"));
    assertEquals(7L, ObjectPathNaming.parseVersion("123_7.bin"));
    assertFalse(ObjectPathNaming.isLegacyBin("123_7.bin"));

    assertEquals(123L, ObjectPathNaming.parseTime("123.bin.tmp"));
    assertEquals(ObjectPathNaming.LEGACY_VERSION, ObjectPathNaming.parseVersion("123.bin.tmp"));

    assertEquals(123L, ObjectPathNaming.parseTime("123.bin.back"));
    assertEquals(-1L, ObjectPathNaming.parseTime("abc.bin"));
    assertEquals(-1L, ObjectPathNaming.parseTime("123.tmp"));
  }

  @Test
  public void buildNamesAndRelativePaths() {
    assertEquals("123_9.bin", ObjectPathNaming.toVersionedFileName(123L, 9L));
    assertEquals("123.bin.tmp", ObjectPathNaming.toTempFileName(123L));
    String relative = "9/tbl/tag/col/123.bin";
    assertTrue(ObjectPathNaming.withTsFileVersion(relative, 123L, 4L).endsWith("123_4.bin"));
    assertTrue(ObjectPathNaming.toTempRelativePath(relative, 123L).endsWith("123.bin.tmp"));
  }

  @Test
  public void baseFileNameAndRelativize() {
    assertEquals("123_7.bin", ObjectPathNaming.baseFileName("9/tbl/col/123_7.bin"));
    assertEquals(
        "123_7.bin", ObjectPathNaming.baseFileName("os://bucket/9/object/1/tbl/col/123_7.bin"));
    assertEquals("123.bin.tmp", ObjectPathNaming.baseFileName("123.bin.tmp"));
    assertEquals(
        "1/tbl/col/123_7.bin",
        ObjectPathNaming.relativize(
            "os://bucket/dn/object", "os://bucket/dn/object/1/tbl/col/123_7.bin"));
  }
}
