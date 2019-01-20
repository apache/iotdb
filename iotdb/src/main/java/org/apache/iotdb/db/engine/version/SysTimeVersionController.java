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

/**
 * SysTimeVersionController uses system timestamp as the version number.
 */
public class SysTimeVersionController implements VersionController {

  public static final SysTimeVersionController INSTANCE = new SysTimeVersionController();

  private SysTimeVersionController() {

  }

  @Override
  public long nextVersion() {
    return System.currentTimeMillis();
  }

  @Override
  public long currVersion() {
    return System.currentTimeMillis();
  }
}
