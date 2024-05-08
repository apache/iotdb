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
package org.apache.iotdb.tsfile.utils;

import org.apache.iotdb.tsfile.constant.TestConstant;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

  @Test
  public void testConvertUnit() {
    long kb = 3 * 1024;
    long mb = kb * 1024;
    long gb = mb * 1024;
    Assert.assertEquals(
        3.0 * 1024, FileUtils.transformUnit(kb, FileUtils.Unit.B), TestConstant.double_min_delta);
    assertEquals(3, FileUtils.transformUnit(kb, FileUtils.Unit.KB), TestConstant.double_min_delta);

    assertEquals(3, FileUtils.transformUnit(mb, FileUtils.Unit.MB), TestConstant.double_min_delta);
    assertEquals(3, FileUtils.transformUnit(gb, FileUtils.Unit.GB), TestConstant.double_min_delta);
  }

  @Test
  public void testConvertToByte() {
    assertEquals(3l, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.B));
    assertEquals(3l * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.KB));
    assertEquals(3l * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.MB));
    assertEquals(
        3l * 1024 * 1024 * 1024, (long) FileUtils.transformUnitToByte(3, FileUtils.Unit.GB));
  }
}
