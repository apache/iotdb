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

package org.apache.iotdb.db.metadata.plan.schemaregion.impl;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.db.metadata.plan.schemaregion.write.ISetTemplatePlan;

public class SetTemplatePlanImpl implements ISetTemplatePlan {

  private String templateName;

  private String prefixPath;

  public SetTemplatePlanImpl() {}

  public SetTemplatePlanImpl(String templateName, String prefixPath) throws IllegalPathException {

    String[] pathNodes = PathUtils.splitPathToDetachedNodes(prefixPath);
    for (String s : pathNodes) {
      if (s.equals(IoTDBConstant.ONE_LEVEL_PATH_WILDCARD)
          || s.equals(IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD)) {
        throw new IllegalPathException(
            prefixPath, "template cannot be set on a path with wildcard.");
      }
    }

    this.templateName = templateName;
    this.prefixPath = prefixPath;
  }

  @Override
  public String getTemplateName() {
    return templateName;
  }

  @Override
  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  @Override
  public String getPrefixPath() {
    return prefixPath;
  }

  @Override
  public void setPrefixPath(String prefixPath) {
    this.prefixPath = prefixPath;
  }
}
