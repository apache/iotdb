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

package org.apache.iotdb.commons.service.external;

import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;

import java.io.File;
import java.io.IOException;

public class ServiceExecutableManager extends ExecutableManager {
  private ServiceExecutableManager(String temporaryLibRoot, String serviceLibRoot) {
    super(temporaryLibRoot, serviceLibRoot);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static ServiceExecutableManager INSTANCE = null;

  public static synchronized ServiceExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String serviceLibRoot) throws IOException {
    if (INSTANCE == null) {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(serviceLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(serviceLibRoot + File.separator + INSTALL_DIR);
      INSTANCE = new ServiceExecutableManager(temporaryLibRoot, serviceLibRoot);
    }
    return INSTANCE;
  }

  public static ServiceExecutableManager getInstance() {
    return INSTANCE;
  }
}
