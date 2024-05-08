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

package org.apache.iotdb.db.engine.version;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SysTimeVersionControllerTest {

  @Test
  public void test() {
    VersionController versionController = SysTimeVersionController.INSTANCE;
    long diff = versionController.currVersion() - System.currentTimeMillis();
    // to aovid the test failure on a poor machine, we bear 200ms difference here.
    assertTrue(diff >= -200 && diff <= 200);
    diff = versionController.nextVersion();
    try {
      Thread.sleep(200);
      diff -= System.currentTimeMillis();
      assertTrue(diff >= -1000 && diff <= -200);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      // do nothing
    }
  }
}
