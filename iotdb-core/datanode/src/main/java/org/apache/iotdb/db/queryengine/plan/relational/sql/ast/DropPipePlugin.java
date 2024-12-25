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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class DropPipePlugin extends PipeStatement {

  private final String pluginName;
  private final boolean ifExistsCondition;

  public DropPipePlugin(final String pluginName, final boolean ifExistsCondition) {
    this.pluginName = requireNonNull(pluginName, "plugin name can not be null");
    this.ifExistsCondition = ifExistsCondition;
  }

  public String getPluginName() {
    return pluginName;
  }

  public boolean hasIfExistsCondition() {
    return ifExistsCondition;
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitDropPipePlugin(this, context);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pluginName, ifExistsCondition);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    DropPipePlugin other = (DropPipePlugin) obj;
    return Objects.equals(pluginName, other.pluginName)
        && Objects.equals(ifExistsCondition, other.ifExistsCondition);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("pluginName", pluginName)
        .add("ifExistsCondition", ifExistsCondition)
        .toString();
  }
}
