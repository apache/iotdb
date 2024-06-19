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

package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.file.SystemPropertiesHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class ConfigNodeSystemPropertiesHandler extends SystemPropertiesHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(ConfigNodeSystemPropertiesHandler.class);

  private ConfigNodeSystemPropertiesHandler(String filePath) {
    super(filePath);
  }

  public static synchronized SystemPropertiesHandler getInstance() {
    if (Holder.INSTANCE == null) {
      Holder.INSTANCE =
          new ConfigNodeSystemPropertiesHandler(
              ConfigNodeDescriptor.getInstance().getConf().getSystemDir()
                  + File.separator
                  + ConfigNodeConstant.SYSTEM_FILE_NAME);
      Holder.INSTANCE.recover();
    }
    return Holder.INSTANCE;
  }

  private static class Holder {
    private static ConfigNodeSystemPropertiesHandler INSTANCE;
  }
}
