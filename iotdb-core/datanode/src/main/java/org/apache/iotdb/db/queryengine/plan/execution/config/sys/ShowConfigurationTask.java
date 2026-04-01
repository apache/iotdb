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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.ConfigurationFileUtils;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.sys.ShowConfigurationStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowConfigurationTask implements IConfigTask {
  private final ShowConfigurationStatement showConfigurationStatement;

  public ShowConfigurationTask(ShowConfigurationStatement showConfigurationStatement) {
    this.showConfigurationStatement = showConfigurationStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showAppliedConfigurations(showConfigurationStatement);
  }

  public static void buildTsBlock(
      Map<String, String> lastAppliedProperties,
      boolean showAllConfigurations,
      boolean withDescription,
      Collection<PrivilegeType> missingPrivileges,
      SettableFuture<ConfigTaskResult> future)
      throws IOException {
    List<ColumnHeader> columnHeaders =
        withDescription
            ? ColumnHeaderConstant.SHOW_CONFIGURATIONS_COLUMN_HEADERS_WITH_DESCRIPTION
            : ColumnHeaderConstant.SHOW_CONFIGURATIONS_COLUMN_HEADERS;
    TsBlockBuilder builder =
        new TsBlockBuilder(
            columnHeaders.stream().map(ColumnHeader::getColumnType).collect(Collectors.toList()));
    if (lastAppliedProperties != null) {
      for (ConfigurationFileUtils.DefaultConfigurationItem item :
          ConfigurationFileUtils.getConfigurationItemsFromTemplate(withDescription).values()) {
        String name = item.name;
        String value = lastAppliedProperties.get(name);
        if (!showAllConfigurations && value == null) {
          continue;
        }
        if (missingPrivileges.contains(item.privilege)) {
          continue;
        }
        builder.getTimeColumnBuilder().writeLong(0L);
        String defaultValue = item.value;
        String description = item.description;
        addValueToTsBlockBuilder(builder, 0, name);
        addValueToTsBlockBuilder(builder, 1, value);
        addValueToTsBlockBuilder(builder, 2, defaultValue);
        if (withDescription) {
          addValueToTsBlockBuilder(builder, 3, description);
        }
        builder.declarePosition();
      }
    }
    future.set(
        new ConfigTaskResult(
            TSStatusCode.SUCCESS_STATUS, builder.build(), new DatasetHeader(columnHeaders, true)));
  }

  private static void addValueToTsBlockBuilder(TsBlockBuilder builder, int index, String value) {
    if (value == null) {
      builder.getColumnBuilder(index).appendNull();
    } else {
      builder.getColumnBuilder(index).writeBinary(new Binary(value, TSFileConfig.STRING_CHARSET));
    }
  }
}
