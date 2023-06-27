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

package org.apache.iotdb.commons.utils;

import org.junit.Assert;
import org.junit.Test;

public class JVMCommonUtilsTest {

  @Test
  public void getJdkVersionTest() {
    try {
      System.setProperty("java.version", "1.8.0_233");
      Assert.assertEquals(8, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "11.0.16");
      Assert.assertEquals(11, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "11.0.8-internal");
      Assert.assertEquals(11, JVMCommonUtils.getJdkVersion());
      System.setProperty("java.version", "17-internal");
      Assert.assertEquals(17, JVMCommonUtils.getJdkVersion());
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
