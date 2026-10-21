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
import org.apache.iotdb.commons.exception.MetadataLeaseFencedException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.table.AlterOrDropTableOperationType;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
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
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adds to the tree-to-table views the columns which the measurements a schema write has created
 * need.
 *
 * <p>It is the automatic counterpart of the {@code ALTER VIEW ... ADD COLUMN} statement: the
 * columns come from the measurements the schema write has just created, instead of from the
 * statement itself.
 */
public class ViewColumnAutoSyncTrigger {

  // Public so that the schema write path, which invokes sync and catches its failures, logs them
  // with the logger of the feature; logback routes this logger to its own error file.
  public static final Logger LOGGER = LoggerFactory.getLogger(ViewColumnAutoSyncTrigger.class);
  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public static final String QUERY_ID_PREFIX = "tree_to_table_add_column";
  public static final String EXTEND_DEVICE_TEMPLATE_ID_PREFIX = "template_";

  /** How many devices, and how many measurements per device, a fenced lease log line shows. */
  private static final int MAX_LOGGED_DEVICES = 10;

  private static final int MAX_LOGGED_MEASUREMENTS_PER_DEVICE = 10;

  /**
   * Adds the columns the views covering the given devices need.
   *
   * <p>It is a best-effort side effect of the write which triggers it: every failure is logged here
   * and none of them is propagated, so that the write operation is never affected.
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
    final Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements;
    try {
      view2Measurements = collectViewMeasurements(device2Measurements);
    } catch (final MetadataLeaseFencedException e) {
      // The DataNode is isolated: neither the index nor the table cache it consults may be used.
      // Give up this round and record the measurements which cannot be added while it is fenced.
      LOGGER.error(
          DataNodeSchemaMessages
              .LOG_THE_METADATA_LEASE_IS_FENCED_WHILE_ADDING_VIEW_COLUMNS_GIVING_UP_THIS_ROUND_THE_MEASUREMENTS_ARE_ARG_5D1B2D44,
          formatDeviceMeasurements(device2Measurements));
      return;
    }
    if (view2Measurements.isEmpty()) {
      return;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(AbstractConfigNodeInfo.CONFIG_REGION_ID)) {
      List<TAlterOrDropTableReq> updateViewReqs =
          view2Measurements.entrySet().stream()
              .map(ViewColumnAutoSyncTrigger::toAlterTableReq)
              .toList();

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
   * A bounded, human readable form of the measurements, so that the log line of a batch write stays
   * short: at most {@link #MAX_LOGGED_DEVICES} devices and {@link
   * #MAX_LOGGED_MEASUREMENTS_PER_DEVICE} measurements per device are written, the rest is
   * summarized.
   */
  private static String formatDeviceMeasurements(
      final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements) {
    final StringBuilder builder = new StringBuilder("{");
    int loggedDevices = 0;
    for (final Map.Entry<PartialPath, List<Pair<String, TSDataType>>> entry :
        device2Measurements.entrySet()) {
      if (loggedDevices == MAX_LOGGED_DEVICES) {
        builder.append("... (").append(device2Measurements.size()).append(" devices in total)}");
        return builder.toString();
      }
      if (loggedDevices > 0) {
        builder.append(", ");
      }
      builder.append(entry.getKey().getFullPath()).append("=[");
      final List<Pair<String, TSDataType>> measurements = entry.getValue();
      for (int i = 0; i < measurements.size(); i++) {
        if (i == MAX_LOGGED_MEASUREMENTS_PER_DEVICE) {
          builder.append("... (").append(measurements.size()).append(" measurements in total)");
          break;
        }
        if (i > 0) {
          builder.append(", ");
        }
        builder
            .append(measurements.get(i).getLeft())
            .append(':')
            .append(measurements.get(i).getRight());
      }
      builder.append(']');
      loggedDevices++;
    }
    return builder.append('}').toString();
  }

  /**
   * @return the measurements each view covering the given devices still needs; eg: view01 -> {s1,
   *     s2, s3}, view02 -> {s1, s2, s3, s4}
   */
  private static Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>>
      collectViewMeasurements(
          Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements) {

    // deduplicate the measurement by set, the measurement with same name and dataType
    // from different device would count once, eg: dev01.s1(int32)  and dev02.s1(int32)
    // the measurements which already have a column in the view are dropped below
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

    // A measurement the view already has a column for is not needed again: ConfigNode would drop
    // it in its column check and reject the request when nothing is left, which costs an RPC and a
    // persisted procedure for nothing. A view the table cache does not hold is left as it is, as
    // the DataNode cannot tell which columns are missing then.
    final Iterator<Map.Entry<TreeToTableViewReference, Set<Pair<String, TSDataType>>>> iterator =
        view2Measurements.entrySet().iterator();
    while (iterator.hasNext()) {
      final Map.Entry<TreeToTableViewReference, Set<Pair<String, TSDataType>>> viewEntry =
          iterator.next();
      final TsTable view = getCachedView(viewEntry.getKey());
      if (Objects.isNull(view)) {
        continue;
      }
      removeExistingMeasurements(view, viewEntry.getValue());
      if (viewEntry.getValue().isEmpty()) {
        iterator.remove();
      }
    }
    return view2Measurements;
  }

  /** The view as the DataNode caches it, or null when the cache does not hold it. */
  private static TsTable getCachedView(final TreeToTableViewReference view) {
    return DataNodeTableCache.getInstance()
        .getTable(view.getDatabaseName(), view.getTableName(), false);
  }

  /** Drops the measurements which the view already has a column for. */
  private static void removeExistingMeasurements(
      final TsTable view, final Set<Pair<String, TSDataType>> measurements) {
    // The same test ConfigNode applies before it adds the columns: the column name, or the source
    // measurement a FROM column references. A measurement the cache does not know yet is kept, as
    // the cache may lag behind ConfigNode.
    final Set<String> sourceNames = TreeViewSchema.getSourceNames(view);
    measurements.removeIf(
        measurement ->
            Objects.nonNull(view.getColumnSchema(measurement.getLeft().toLowerCase(Locale.ROOT)))
                || sourceNames.contains(measurement.getLeft()));
  }

  private static TAlterOrDropTableReq toAlterTableReq(
      Map.Entry<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements) {
    final TreeToTableViewReference view = view2Measurements.getKey();
    final List<TsTableColumnSchema> columnSchemas =
        view2Measurements.getValue().stream()
            .map(ViewColumnAutoSyncTrigger::toColumnSchema)
            .toList();
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
  // the same add view column request. The database and table names may contain any character, so
  // their lengths are written out to keep the parts apart: without them, the views a_b.c and
  // a.b_c would share an id and the request for the second one would be taken for a repeat of the
  // first one's.
  private static String buildQueryId(String columnFingerprint, String database, String table) {
    return String.join(
        "_",
        QUERY_ID_PREFIX,
        columnFingerprint,
        String.valueOf(database.length()),
        database,
        String.valueOf(table.length()),
        table);
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
