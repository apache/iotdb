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

package com.timecho.iotdb.confignode.consensus.request;

import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;

import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AddWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.AlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.CommitDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreAlterWritableViewColumnDataTypePlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.PreDeleteWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewColumnPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RenameWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.RollbackCreateWritableViewPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewColumnCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewCommentPlan;
import com.timecho.iotdb.confignode.consensus.request.write.table.view.writable.SetWritableViewPropertiesPlan;

import java.io.IOException;

public final class TimechoConfigPhysicalPlanFactory {

  public static boolean isTimechoPlanType(final ConfigPhysicalPlanType configPhysicalPlanType) {
    return configPhysicalPlanType.getPlanType() < 0;
  }

  public static ConfigPhysicalPlan create(final ConfigPhysicalPlanType configPhysicalPlanType)
      throws IOException {
    switch (configPhysicalPlanType) {
      case RollbackCreateWritableView:
        return new RollbackCreateWritableViewPlan();
      case CommitCreateWritableView:
        return new CommitCreateWritableViewPlan();
      case AddWritableViewColumn:
        return new AddWritableViewColumnPlan();
      case CommitDeleteWritableViewColumn:
        return new CommitDeleteWritableViewColumnPlan();
      case CommitDeleteWritableView:
        return new CommitDeleteWritableViewPlan();
      case RenameWritableView:
        return new RenameWritableViewPlan();
      case SetWritableViewProperties:
        return new SetWritableViewPropertiesPlan();
      case PreDeleteWritableViewColumn:
        return new PreDeleteWritableViewColumnPlan();
      case PreDeleteWritableView:
        return new PreDeleteWritableViewPlan();
      case RenameWritableViewColumn:
        return new RenameWritableViewColumnPlan();
      case PreAlterWritableViewColumnDataType:
        return new PreAlterWritableViewColumnDataTypePlan();
      case AlterWritableViewColumnDataType:
        return new AlterWritableViewColumnDataTypePlan();
      case SetWritableViewColumnComment:
        return new SetWritableViewColumnCommentPlan();
      case SetWritableViewComment:
        return new SetWritableViewCommentPlan();
      default:
        throw new IOException(
            String.format(
                ProcedureMessages.UNKNOWN_TIMECHO_WRITABLE_VIEW_PHYSICAL_PLAN_CONFIG_TYPE,
                configPhysicalPlanType));
    }
  }

  private TimechoConfigPhysicalPlanFactory() {
    // Empty constructor
  }
}
