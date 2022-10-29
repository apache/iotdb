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

package org.apache.iotdb.commons.trigger.service;

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;

import java.io.File;
import java.io.IOException;

public class TriggerExecutableManager extends ExecutableManager {
  private TriggerExecutableManager(String temporaryLibRoot, String triggerLibRoot) {
    super(temporaryLibRoot, triggerLibRoot);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static TriggerExecutableManager INSTANCE = null;

  public static synchronized TriggerExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String triggerLibRoot) throws IOException {
    if (INSTANCE == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(triggerLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(triggerLibRoot + File.separator + INSTALL_DIR);
      INSTANCE = new TriggerExecutableManager(temporaryLibRoot, triggerLibRoot);
    }
    return INSTANCE;
  }

  public static TriggerExecutableManager getInstance() {
    return INSTANCE;
  }
}
