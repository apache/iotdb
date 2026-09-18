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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.AbstractConfigNodeInfo;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.commons.schema.table.AlterOrDropTableOperationType;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnCategory;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchemaUtil;
import org.apache.iotdb.commons.schema.template.Template;
import org.apache.iotdb.confignode.rpc.thrift.TAlterOrDropTableReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateTree2TableViewFailure;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateTree2TableViewsReq;
import org.apache.iotdb.confignode.rpc.thrift.TUpdateTree2TableViewsResp;
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.ActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.BatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateAlignedTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.CreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalBatchActivateTemplateNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateMultiTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.InternalCreateTimeSeriesNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.metadata.write.MeasurementGroup;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Adds the columns which the tree-to-table views covering the created timeseries need.
 *
 * <p>The trigger is a best-effort side effect of the schema creation, see addViewColumnsIfNeeded.
 */
public class TriggerAddViewColumnVisitor
    implements PlanVisitor<TSStatus, TriggerAddViewColumnVisitor.Context> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerAddViewColumnVisitor.class);
  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  public static final String QUERY_ID_PREFIX = "tree_to_table_add_column";
  public static final String ALTER_DEVICE_TEMPLATE_ID_PREFIX = "template_";

  public static final class Context {

    private final Map<Integer, Template> templates;

    public Context(Map<Integer, Template> templates) {
      this.templates = Objects.isNull(templates) ? Collections.emptyMap() : templates;
    }

    public Map<Integer, Template> getTemplates() {
      return templates;
    }
  }

  /**
   * Adds the columns the views covering the given devices need.
   *
   * <p>It is a best-effort side effect of the write which triggers it: every failure is logged here
   * and none of them is propagated, so that the write operation is never affected. The columns
   * which are not added this time are added by a later write of the same measurements, as every
   * write plan carries its own planNodeId, hence its own queryId.
   *
   * @param device2Measurements eg. device01 -> {s1, s2, s3}; device02 -> {s3, s4}
   * @param operationId the planNodeId of the plan which triggers this operation
   */
  public static void addViewColumnsIfNeeded(
      Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements, String operationId) {
    if (device2Measurements == null || device2Measurements.isEmpty()) {
      return;
    }
    final Map<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements =
        collectViewMeasurements(device2Measurements);
    if (view2Measurements.isEmpty()) {
      return;
    }

    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(AbstractConfigNodeInfo.CONFIG_REGION_ID)) {
      List<TAlterOrDropTableReq> updateViewReqs =
          view2Measurements.entrySet().stream()
              .map(entry -> TriggerAddViewColumnVisitor.toAlterTableReq(entry, operationId))
              .toList();

      TUpdateTree2TableViewsResp resp =
          configNodeClient.updateTree2TableViews(new TUpdateTree2TableViewsReq(updateViewReqs));
      if (resp == null || resp.getStatus() == null) {
        LOGGER.error(DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_EMPTY_RESPONSE, operationId);
        return;
      }
      if (resp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        LOGGER.error(
            DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_CN_FAILED, operationId, resp.getStatus());
      }
      // the views are updated one by one in ConfigNode, so the failed ones are reported one by one
      if (resp.getFailedViews() != null) {
        for (TUpdateTree2TableViewFailure failedView : resp.getFailedViews()) {
          LOGGER.error(
              DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_FAILED_FOR_VIEW,
              failedView.getDatabase(),
              failedView.getTableName(),
              operationId,
              failedView.getStatus());
        }
      }
    } catch (ClientManagerException | TException e) {
      LOGGER.warn(DataNodeSchemaMessages.LOG_ADD_VIEW_COLUMN_CN_UNREACHABLE, operationId, e);
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
      Map.Entry<TreeToTableViewReference, Set<Pair<String, TSDataType>>> view2Measurements,
      String operationId) {
    final TreeToTableViewReference view = view2Measurements.getKey();
    final List<TsTableColumnSchema> columnSchemas =
        view2Measurements.getValue().stream()
            .map(TriggerAddViewColumnVisitor::toColumnSchema)
            .toList();
    TAlterOrDropTableReq tAlterOrDropTableReq =
        new TAlterOrDropTableReq(
            view.getDatabaseName(),
            view.getTableName(),
            buildQueryId(
                operationId + "_" + columnFingerprint(columnSchemas),
                view.getDatabaseName(),
                view.getTableName()),
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

  @Override
  public TSStatus visitCreateTimeSeries(
      CreateTimeSeriesNode node, TriggerAddViewColumnVisitor.Context context) {
    final MeasurementPath path = node.getPath();
    final Pair<String, TSDataType> measurement =
        new Pair<>(path.getMeasurement(), node.getDataType());
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        Collections.singletonMap(path.getDevicePath(), Collections.singletonList(measurement));
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitCreateAlignedTimeSeries(
      CreateAlignedTimeSeriesNode node, TriggerAddViewColumnVisitor.Context context) {
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        Collections.singletonMap(
            node.getDevicePath(), toMeasurements(node.getMeasurements(), node.getDataTypes()));
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitCreateMultiTimeSeries(
      CreateMultiTimeSeriesNode node, TriggerAddViewColumnVisitor.Context context) {
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        toDevice2Measurements(node.getMeasurementGroupMap());
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitInternalCreateTimeSeries(
      InternalCreateTimeSeriesNode node, TriggerAddViewColumnVisitor.Context context) {
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        Collections.singletonMap(node.getDevicePath(), toMeasurements(node.getMeasurementGroup()));
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitInternalCreateMultiTimeSeries(
      InternalCreateMultiTimeSeriesNode node, TriggerAddViewColumnVisitor.Context context) {
    // the alignment flag is of no use to a view, only the measurements of the devices matter
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        new HashMap<>(node.getDeviceMap().size());
    for (final Map.Entry<PartialPath, Pair<Boolean, MeasurementGroup>> entry :
        node.getDeviceMap().entrySet()) {
      device2Measurements.put(entry.getKey(), toMeasurements(entry.getValue().getRight()));
    }
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitActivateTemplate(
      ActivateTemplateNode node, TriggerAddViewColumnVisitor.Context context) {
    final Template template = context.getTemplates().get(node.getTemplateId());
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        Collections.singletonMap(node.getActivatePath(), toMeasurements(template));
    addViewColumnsIfNeeded(device2Measurements, node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitBatchActivateTemplate(
      BatchActivateTemplateNode node, TriggerAddViewColumnVisitor.Context context) {
    addViewColumnsIfNeeded(
        toActivationDevice2Measurements(node.getTemplateActivationMap(), context),
        node.getPlanNodeId().getId());
    return null;
  }

  @Override
  public TSStatus visitInternalBatchActivateTemplate(
      InternalBatchActivateTemplateNode node, TriggerAddViewColumnVisitor.Context context) {
    addViewColumnsIfNeeded(
        toActivationDevice2Measurements(node.getTemplateActivationMap(), context),
        node.getPlanNodeId().getId());
    return null;
  }

  private static Map<PartialPath, List<Pair<String, TSDataType>>> toActivationDevice2Measurements(
      final Map<PartialPath, Pair<Integer, Integer>> templateActivationMap,
      final TriggerAddViewColumnVisitor.Context context) {
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        new HashMap<>(templateActivationMap.size());
    for (final Map.Entry<PartialPath, Pair<Integer, Integer>> entry :
        templateActivationMap.entrySet()) {
      final Template template = context.getTemplates().get(entry.getValue().getLeft());
      if (Objects.isNull(template)) {
        continue;
      }
      device2Measurements.put(entry.getKey(), toMeasurements(template));
    }
    return device2Measurements;
  }

  private static Map<PartialPath, List<Pair<String, TSDataType>>> toDevice2Measurements(
      final Map<PartialPath, MeasurementGroup> measurementGroupMap) {
    final Map<PartialPath, List<Pair<String, TSDataType>>> device2Measurements =
        new HashMap<>(measurementGroupMap.size());
    for (final Map.Entry<PartialPath, MeasurementGroup> entry : measurementGroupMap.entrySet()) {
      device2Measurements.put(entry.getKey(), toMeasurements(entry.getValue()));
    }
    return device2Measurements;
  }

  private static List<Pair<String, TSDataType>> toMeasurements(final Template template) {
    return template.getSchemaMap().values().stream()
        .map(schema -> new Pair<>(schema.getMeasurementName(), schema.getType()))
        .toList();
  }

  private static List<Pair<String, TSDataType>> toMeasurements(MeasurementGroup measurementGroup) {
    return toMeasurements(measurementGroup.getMeasurements(), measurementGroup.getDataTypes());
  }

  private static List<Pair<String, TSDataType>> toMeasurements(
      final List<String> measurements, final List<TSDataType> dataTypes) {
    final List<Pair<String, TSDataType>> result = new ArrayList<>(measurements.size());
    for (int i = 0; i < measurements.size(); i++) {
      result.add(new Pair<>(measurements.get(i), dataTypes.get(i)));
    }
    return result;
  }

  @Override
  public TSStatus visitPlan(PlanNode node, TriggerAddViewColumnVisitor.Context context) {
    throw new UnsupportedOperationException();
  }

  /**
   * The queryId identifies one column-adding operation in ConfigNode, so as to deduplicate the
   * requests sent by the leader and the followers while applying the same schema region log entry.
   *
   * <p>The {@code operationId} is the planNodeId of the plan which triggers the operation, and it
   * is identical on every replica of one log entry. The database, the table and the fingerprint of
   * the columns are appended because ConfigNode only compares the queryId, instead of the table,
   * while deduplicating: without them the operation of another view, or of another column set,
   * would be treated as a duplicate and silently skipped.
   */
  private static String buildQueryId(String operationId, String database, String table) {
    return QUERY_ID_PREFIX + "_" + operationId + "_" + database + "_" + table;
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
