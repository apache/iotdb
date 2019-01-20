/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.engine.version;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class SysTimeVersionControllerTest {

  @Test
  public void test() {
    VersionController versionController = SysTimeVersionController.INSTANCE;
    long diff = versionController.currVersion() - System.currentTimeMillis();
    assertTrue(diff >= -1 && diff <= 1);
    diff = versionController.nextVersion() - System.currentTimeMillis();
    assertTrue(diff >= -1 && diff <= 1);
  }
}