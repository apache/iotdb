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
package org.apache.iotdb.it.env.cluster;

import org.apache.iotdb.itbase.env.DataNodeConfig;

import java.io.IOException;

public class MppDataNodeConfig extends MppBaseConfig implements DataNodeConfig {

  public MppDataNodeConfig() {
    super();
  }

  public MppDataNodeConfig(String filePath) throws IOException {
    super(filePath);
  }

  @Override
  public MppBaseConfig emptyClone() {
    return new MppDataNodeConfig();
  }

  @Override
  public void updateProperties(MppBaseConfig persistentConfig) {
    if (persistentConfig instanceof MppDataNodeConfig) {
      super.updateProperties(persistentConfig);
    } else {
      throw new UnsupportedOperationException(
          "MppDataNodeConfig can't be override by an instance of "
              + persistentConfig.getClass().getCanonicalName());
    }
  }
}
