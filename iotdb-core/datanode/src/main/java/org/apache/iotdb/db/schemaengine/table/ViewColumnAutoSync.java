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

package org.apache.iotdb.db.schemaengine.table;

import org.apache.iotdb.commons.client.AbstractConfigNodeInfo;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.AlterOrDropTableOperationType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TAutoSyncViewColumnsFailure;
import org.apache.iotdb.confignode.rpc.thrift.TAutoSyncViewColumnsReq;
import org.apache.iotdb.confignode.rpc.thrift.TAutoSyncViewColumnsResp;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.fetcher.TableHeaderSchemaValidator;
import org.apache.iotdb.db.schemaengine.schemaregion.view.TreeToTableViewReference;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.TException;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Adds to the tree-to-table views the columns which the measurements a schema write has created
 * need.
 *
 * <p>It is the automatic counterpart of the {@code ALTER VIEW ... ADD COLUMN} statement: the
 * columns come from the measurements the schema write has just created, instead of from the
 * statement itself.
 */
public class ViewColumnAutoSync {

  private static final Logger LOGGER = LoggerFactory.getLogger(ViewColumnAutoSync.class);
  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public static final String QUERY_ID_PREFIX = "tree_to_table_add_column";
  public static final String EXTEND_DEVICE_TEMPLATE_ID_PREFIX = "template_";

  /**
   * Adds the columns the views covering the given devices need.
   *
   * <p>It is a best-effort side effect of the write which triggers it: every failure is logged here
   * and none of them is propagated, so that the write operation is never affected. The columns
   * which are not added this time are only added again when a later schema write covers the same
   * measurements.
   *
   * @param device2Measurements the measurements the schema write really created, eg. device01 ->
   *     {s1, s2, s3}; device02 -> {s3, s4}. Only they get a column, so that a creation which failed
   *     cannot leave a column behind.
   * @param operationId the planNodeId of the plan which triggers this operation, for the log
   *     messages only: it is a per-query index, so it does not identify the operation
   */
  public static void sync(
      final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements,
      final String operationId) {
    if (device2Measurements == null || device2Measurements.isEmpty()) {
      return;
    }

    // the views to be added columns
    final Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements =
        collectViewMeasurements(device2Measurements);
    if (view2Measurements.isEmpty()) {
      return;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(AbstractConfigNodeInfo.CONFIG_REGION_ID)) {
      List<TAlterOrDropTableReq> updateViewReqs =
          view2Measurements.entrySet().stream().map(ViewColumnAutoSync::toAlterTableReq).toList();

      TAutoSyncViewColumnsResp resp =
          configNodeClient.autoSyncViewColumns(new TAutoSyncViewColumnsReq(updateViewReqs));
      if (resp == null || resp.getStatus() == null) {
        LOGGER.error(
            DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_EMPTY_RESPONSE_B9502E26, operationId);
        return;
      }
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_CN_FAILED_FE4F10AC,
            operationId,
            resp.getStatus());
      }
      // the views are updated one by one in ConfigNode, so the failed ones are reported one by one
      if (resp.getFailedViews() != null) {
        for (TAutoSyncViewColumnsFailure failedView : resp.getFailedViews()) {
          LOGGER.error(
              DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_FAILED_FOR_VIEW_B3EB8144,
              failedView.getDatabase(),
              failedView.getTableName(),
              operationId,
              failedView.getStatus());
        }
      }
    } catch (ClientManagerException | TException e) {
      LOGGER.warn(
          DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_CN_UNREACHABLE_E9BC7652, operationId, e);
    }
  }

  /**
   * @return the measurements each view covering the given devices needs; eg: view01 -> {s1, s2,
   *     s3}, view02 -> {s1, s2, s3, s4}
   */
  private static Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>>
      collectViewMeasurements(
          Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements) {

    // deduplicate the measurement by set, the measurement with same name and dataType
    // from different device would count once, eg: dev01.s1(int32)  and dev02.s1(int32)
    // other deduplication would leave to ConfigMTree in CN
    final Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements =
        new HashMap<>();

    for (Map.Entry<PartialPath, List<Pair<String, TSDataType>>> entry :
        device2Measurements.entrySet()) {
      PartialPath path = entry.getKey();
      Set<TreeToTableViewReference> treeToTableViews =
          TreeToTableViewCache.getInstance().getTreeToTableViews(path);

      treeToTableViews.forEach(
          view ->
              view2Measurements
                  .computeIfAbsent(view, key -> new LinkedHashSet<>())
                  .addAll(entry.getValue()));
    }
    return view2Measurements;
  }

  private static TAlterOrDropTableReq toAlterTableReq(
      Map.Entry<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements) {
    final TreeToTableViewReference view = view2Measurements.getKey();
    final List<TsTableColumnSchema> columnSchemas =
        view2Measurements.getValue().stream().map(ViewColumnAutoSync::toColumnSchema).toList();
    TAlterOrDropTableReq tAlterOrDropTableReq =
        new TAlterOrDropTableReq(
            view.getDatabaseName(),
            view.getTableName(),
            buildQueryId(
                columnFingerprint(columnSchemas), view.getDatabaseName(), view.getTableName()),
            AlterOrDropTableOperationType.ADD_COLUMN.getTypeValue(),
            ByteBuffer.wrap(TsTableColumnSchemaUtil.serialize(columnSchemas)));
    tAlterOrDropTableReq.setIsView(true);
    return tAlterOrDropTableReq;
  }

  // transform the upperCase to the from-attribute
  private static TsTableColumnSchema toColumnSchema(final Pair<String, TSDataType> measurement) {
    final String measurementName = measurement.getLeft();
    final String from =
        measurementName.chars().anyMatch(Character::isUpperCase) ? measurementName : null;
    return TableHeaderSchemaValidator.generateColumnSchema(
        TsTableColumnCategory.FIELD,
        measurementName.toLowerCase(Locale.ROOT),
        measurement.getRight(),
        null,
        from);
  }

  // leader and follower would build the same queryId when apply the same log; so CN could recognize
  // the same add view column request
  private static String buildQueryId(String columnFingerprint, String database, String table) {
    return QUERY_ID_PREFIX + "_" + columnFingerprint + "_" + database + "_" + table;
  }

  /**
   * The columns are sorted first so that the same column set always yields the same fingerprint, no
   * matter in which order the measurements were collected.
   */
  private static String columnFingerprint(final List<TsTableColumnSchema> columnSchemas) {
    final List<TsTableColumnSchema> sortedColumnSchemas = new ArrayList<>(columnSchemas);
    sortedColumnSchemas.sort(Comparator.comparing(TsTableColumnSchema::getColumnName));
    return digestOf(TsTableColumnSchemaUtil.serialize(sortedColumnSchemas));
  }

  private static String digestOf(final byte[] content) {
    try {
      // 4 bytes are enough here, the number of distinct column sets of one view is rather small
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content), 0, 4);
    } catch (final NoSuchAlgorithmException e) {
      // SHA-256 is supported by every Java platform
      throw new IllegalStateException(e);
    }
  }
}
