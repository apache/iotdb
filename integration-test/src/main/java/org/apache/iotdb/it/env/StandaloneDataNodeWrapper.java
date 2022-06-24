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
package org.apache.iotdb.it.env;

import org.apache.commons.lang3.SystemUtils;

import java.io.File;

public class StandaloneDataNodeWrapper extends DataNodeWrapper {

  public StandaloneDataNodeWrapper(
      String targetConfigNode, String testClassName, String testMethodName, int[] portList) {
    super(targetConfigNode, testClassName, testMethodName, portList);
  }

  @Override
  protected String getStartScriptPath() {
    String scriptName = "start-new-server.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "start-new-server.bat";
    }
    return workDirFilePath("datanode" + File.separator + "sbin", scriptName);
  }

  @Override
  protected String getStopScriptPath() {
    String scriptName = "stop-server.sh";
    if (SystemUtils.IS_OS_WINDOWS) {
      scriptName = "stop-server.bat";
    }
    return workDirFilePath("datanode" + File.separator + "sbin", scriptName);
  }
}
