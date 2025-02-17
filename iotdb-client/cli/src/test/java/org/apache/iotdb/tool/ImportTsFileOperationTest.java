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

package org.apache.iotdb.tool;

import org.apache.iotdb.tool.common.ImportTsFileOperation;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ImportTsFileOperationTest {

  @Test
  public void testIsValidOperation() {
    assertTrue(ImportTsFileOperation.isValidOperation("none"));
    assertTrue(ImportTsFileOperation.isValidOperation("mv"));
    assertTrue(ImportTsFileOperation.isValidOperation("cp"));
    assertTrue(ImportTsFileOperation.isValidOperation("delete"));
    assertFalse(ImportTsFileOperation.isValidOperation("invalid"));
  }

  @Test
  public void testGetOperation() {
    assertEquals(ImportTsFileOperation.NONE, ImportTsFileOperation.getOperation("none", false));
    assertEquals(ImportTsFileOperation.MV, ImportTsFileOperation.getOperation("mv", false));
    assertEquals(ImportTsFileOperation.HARDLINK, ImportTsFileOperation.getOperation("cp", true));
    assertEquals(ImportTsFileOperation.CP, ImportTsFileOperation.getOperation("cp", false));
    assertEquals(ImportTsFileOperation.DELETE, ImportTsFileOperation.getOperation("delete", false));
  }
}
