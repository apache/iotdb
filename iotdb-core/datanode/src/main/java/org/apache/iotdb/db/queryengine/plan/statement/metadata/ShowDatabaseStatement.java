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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseInfo;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.Binary;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SHOW DATABASES statement
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW DATABASES prefixPath?
 */
public class ShowDatabaseStatement extends ShowStatement implements IConfigStatement {

  private final PartialPath pathPattern;
  private boolean isDetailed;
  private boolean showSecurityLabel; // Add field for security label display

  public ShowDatabaseStatement(final PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
    this.isDetailed = false;
    this.showSecurityLabel = false; // Initialize to false
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public boolean isDetailed() {
    return isDetailed;
  }

  public void setDetailed(final boolean detailed) {
    isDetailed = detailed;
  }

  public boolean isShowSecurityLabel() {
    return showSecurityLabel;
  }

  public void setShowSecurityLabel(final boolean showSecurityLabel) {
    this.showSecurityLabel = showSecurityLabel;
  }

  public void buildTSBlock(
      final Map<String, TDatabaseInfo> databaseInfoMap,
      final SettableFuture<ConfigTaskResult> future) {

    final List<TSDataType> outputDataTypes;
    if (showSecurityLabel) {
      // When showing security labels, use only database name and security label
      // columns
      outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
    } else {
      // Normal database info display
      outputDataTypes =
          isDetailed
              ? ColumnHeaderConstant.showDatabasesDetailColumnHeaders.stream()
                  .map(ColumnHeader::getColumnType)
                  .collect(Collectors.toList())
              : ColumnHeaderConstant.showDatabasesColumnHeaders.stream()
                  .map(ColumnHeader::getColumnType)
                  .collect(Collectors.toList());
    }

    final TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    for (final Map.Entry<String, TDatabaseInfo> entry :
        databaseInfoMap.entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .collect(Collectors.toList())) {
      final String database = entry.getKey();
      final TDatabaseInfo databaseInfo = entry.getValue();

      builder.getTimeColumnBuilder().writeLong(0L);

      if (showSecurityLabel) {
        // Output database name and security label
        builder.getColumnBuilder(0).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
        Map<String, String> securityLabelMap = databaseInfo.getSecurityLabel();
        String securityLabel = "";
        if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
          // Convert map to string representation
          securityLabel =
              securityLabelMap.entrySet().stream()
                  .map(labelEntry -> labelEntry.getKey() + ":" + labelEntry.getValue())
                  .collect(Collectors.joining(","));
        }
        builder
            .getColumnBuilder(1)
            .writeBinary(new Binary(securityLabel, TSFileConfig.STRING_CHARSET));
      } else {
        // Normal database info output
        builder.getColumnBuilder(0).writeBinary(new Binary(database, TSFileConfig.STRING_CHARSET));
        builder.getColumnBuilder(1).writeInt(databaseInfo.getSchemaReplicationFactor());
        builder.getColumnBuilder(2).writeInt(databaseInfo.getDataReplicationFactor());
        builder.getColumnBuilder(3).writeLong(databaseInfo.getTimePartitionOrigin());
        builder.getColumnBuilder(4).writeLong(databaseInfo.getTimePartitionInterval());
        if (isDetailed) {
          builder.getColumnBuilder(5).writeInt(databaseInfo.getSchemaRegionNum());
          builder.getColumnBuilder(6).writeInt(databaseInfo.getMinSchemaRegionNum());
          builder.getColumnBuilder(7).writeInt(databaseInfo.getMaxSchemaRegionNum());
          builder.getColumnBuilder(8).writeInt(databaseInfo.getDataRegionNum());
          builder.getColumnBuilder(9).writeInt(databaseInfo.getMinDataRegionNum());
          builder.getColumnBuilder(10).writeInt(databaseInfo.getMaxDataRegionNum());
        }
      }
      builder.declarePosition();
    }

    final DatasetHeader datasetHeader;
    if (showSecurityLabel) {
      datasetHeader = DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader();
    } else {
      datasetHeader = DatasetHeaderFactory.getShowDatabaseHeader(isDetailed);
    }
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  /**
   * 构造只包含数据库名和安全标签的 TSBlock
   *
   * @param securityLabelMap Map<数据库名, 标签字符串>
   * @param future 返回结果
   */
  public void buildTSBlockFromSecurityLabel(
      Map<String, String> securityLabelMap, SettableFuture<ConfigTaskResult> future) {
    try {
      // Add debug logging
      System.out.println("buildTSBlockFromSecurityLabel called with map: " + securityLabelMap);
      System.out.println(
          "Map size: " + (securityLabelMap != null ? securityLabelMap.size() : "null"));
      System.out.println("showSecurityLabel flag: " + showSecurityLabel);
      System.out.println("pathPattern: " + pathPattern);

      List<TSDataType> outputDataTypes =
          ColumnHeaderConstant.SHOW_DATABASE_SECURITY_LABEL_COLUMN_HEADERS.stream()
              .map(ColumnHeader::getColumnType)
              .collect(Collectors.toList());
      System.out.println("Output data types: " + outputDataTypes);
      TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);

      if (securityLabelMap != null && !securityLabelMap.isEmpty()) {
        System.out.println("Processing " + securityLabelMap.size() + " entries");
        for (Map.Entry<String, String> entry : securityLabelMap.entrySet()) {
          System.out.println("Processing entry: " + entry.getKey() + " -> " + entry.getValue());
          builder.getTimeColumnBuilder().writeLong(0L);
          builder
              .getColumnBuilder(0)
              .writeBinary(new Binary(entry.getKey(), TSFileConfig.STRING_CHARSET));
          builder
              .getColumnBuilder(1)
              .writeBinary(
                  new Binary(
                      entry.getValue() == null ? "" : entry.getValue(),
                      TSFileConfig.STRING_CHARSET));
          builder.declarePosition();
        }
        System.out.println("Successfully processed all entries");
      } else {
        System.out.println("Security label map is null or empty, no data to add");
      }

      future.set(
          new ConfigTaskResult(
              TSStatusCode.SUCCESS_STATUS,
              builder.build(),
              DatasetHeaderFactory.getShowDatabaseSecurityLabelHeader()));
      System.out.println("Successfully set future result");
    } catch (Exception e) {
      System.out.println("Exception in buildTSBlockFromSecurityLabel: " + e.getMessage());
      e.printStackTrace();
      future.setException(e);
    }
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowStorageGroup(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.READ;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }
}
