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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.i18n.ManagerMessages;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TreeDeviceViewFieldDetector {

  private static final Logger LOGGER = LoggerFactory.getLogger(TreeDeviceViewFieldDetector.class);
  private final ConfigManager configManager;
  private final PartialPath path;
  private final TsTable table;

  private TDeviceViewResp result = new TDeviceViewResp(StatusUtils.OK, new ConcurrentHashMap<>());
  private final Map<String, String> lowerCase2OriginalMap = new HashMap<>();

  public TreeDeviceViewFieldDetector(final ConfigManager configManager, final TsTable table) {
    this.configManager = configManager;
    this.path = TreeViewSchema.getPrefixPattern(table);
    this.table = table;
  }

  /**
   * Fills the fields of the view which is being created: a view may declare no field at all, in
   * which case every field under its tree path pattern becomes a column, or declare fields whose
   * type is left to the detection.
   */
  public TSStatus detectFieldsForCreation() {
    if (table.getFieldNum() == 0) {
      // for the first time creation of the table, user do not assign any field;
      // just detect all the fields of prefix path
      new TreeDeviceViewFieldDetectionTaskExecutor(
              configManager,
              getLatestSchemaRegionMap(),
              table.getTagNum(),
              TreeViewSchema.isRestrict(table),
              null,
              false)
          .execute();
      if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result.getStatus();
      }
      result
          .getDeviewViewFieldTypeMap()
          .forEach(
              (field, type) ->
                  table.addColumnSchema(toFieldColumnSchemaWithOriginName(field, type)));
      return StatusUtils.OK;
    }

    return detectFieldTypes(collectUnknownFieldColumns());
  }

  /**
   * Resolves the type of the field columns which are being added to an existing view. The columns
   * to resolve are given by the caller, as only it knows which ones it is adding.
   */
  public TSStatus detectFieldTypesForColumnAddition(final Map<String, FieldColumnSchema> fields) {
    return detectFieldTypes(fields);
  }

  /**
   * The fields which exist under the view's tree path pattern but are not columns of the view yet,
   * as the columns to add. Used after a view is created, to pick up the series which were created
   * before the view was mounted on the DataNodes, and which the DataNodes therefore never reported.
   */
  public Pair<TSStatus, List<TsTableColumnSchema>> detectNewFieldColumnsAfterCreation() {
    new TreeDeviceViewFieldDetectionTaskExecutor(
            configManager,
            getLatestSchemaRegionMap(),
            table.getTagNum(),
            TreeViewSchema.isRestrict(table),
            null,
            true)
        .execute();
    if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return new Pair<>(result.getStatus(), Collections.emptyList());
    }
    // A source measurement can only be referenced once, so a field whose source the view already
    // references under another column name gets no column either.
    final Set<String> sourceNames = TreeViewSchema.getSourceNames(table);
    final List<TsTableColumnSchema> columns = new ArrayList<>();
    result
        .getDeviewViewFieldTypeMap()
        .forEach(
            (field, type) -> {
              if (Objects.isNull(table.getColumnSchema(field))
                  && !sourceNames.contains(lowerCase2OriginalMap.get(field))) {
                columns.add(toFieldColumnSchemaWithOriginName(field, type));
              }
            });
    return new Pair<>(StatusUtils.OK, columns);
  }

  /**
   * The columns which are missing from a view which auto-syncs its schema: the series which already
   * existed under its tree path pattern were created before the DataNodes mounted the view on their
   * tree-to-table view index, so they never triggered the addition of their column. Used right
   * after the view is mounted, whether it has just been created or its auto detection has just been
   * turned on.
   *
   * @return the columns to add; empty when the view does not auto-sync its schema, or when the
   *     detection itself failed
   */
  public static List<TsTableColumnSchema> detectMissingColumnsAfterCreation(
      final ConfigManager configManager, final String database, final TsTable table) {
    final boolean autoSync =
        table.getPropValue(TsTable.AUTO_SYNC_SCHEMA).map(Boolean::parseBoolean).orElse(false);
    if (!autoSync) {
      return Collections.emptyList();
    }

    final Pair<TSStatus, List<TsTableColumnSchema>> detected =
        new TreeDeviceViewFieldDetector(configManager, table).detectNewFieldColumnsAfterCreation();
    if (detected.getLeft().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      LOGGER.warn(
          ProcedureMessages
              .LOG_FAILED_TO_DETECT_THE_TREE_SERIES_UNDER_THE_NEW_VIEW_ARG_ARG_ARG_8F20CAE7,
          database,
          table.getTableName(),
          detected.getLeft());
      return Collections.emptyList();
    }
    return detected.getRight();
  }

  /** The field columns of the view whose type is not known yet, keyed by their source name. */
  private Map<String, FieldColumnSchema> collectUnknownFieldColumns() {
    final Map<String, FieldColumnSchema> unknownFields = new HashMap<>();
    for (final TsTableColumnSchema schema : table.getColumnList()) {
      if (!(schema instanceof FieldColumnSchema) || schema.getDataType() != TSDataType.UNKNOWN) {
        continue;
      }
      final String key = TreeViewSchema.getSourceName(schema);
      if (unknownFields.containsKey(key)) {
        // A table which references the same source measurement twice is rejected when it is
        // parsed, so a table in the schema must not reach this point.
        throw new SemanticException(
            String.format(
                ManagerMessages
                    .EXCEPTION_THE_DUPLICATED_SOURCE_MEASUREMENT_ARG_IS_UNSUPPORTED_YET_873AF566,
                key));
      }
      unknownFields.put(key, (FieldColumnSchema) schema);
    }
    return unknownFields;
  }

  /**
   * Resolves the type of every field column in {@code unknownFields} from the schema of the regions
   * the view covers. A measurement which the detection cannot find fails the whole detection.
   */
  private TSStatus detectFieldTypes(final Map<String, FieldColumnSchema> unknownFields) {
    if (unknownFields.isEmpty()) {
      return StatusUtils.OK;
    }
    new TreeDeviceViewFieldDetectionTaskExecutor(
            configManager,
            getLatestSchemaRegionMap(),
            table.getTagNum(),
            TreeViewSchema.isRestrict(table),
            unknownFields.keySet(),
            false)
        .execute();
    if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      return result.getStatus();
    }
    for (final Map.Entry<String, FieldColumnSchema> unknownField : unknownFields.entrySet()) {
      // The detection keys the field types by the lower case of the measurement name, while the
      // column keeps the source name as the statement spelled it.
      final String sourceName = unknownField.getKey().toLowerCase(Locale.ENGLISH);
      if (result.getDeviewViewFieldTypeMap().containsKey(sourceName)) {
        unknownField
            .getValue()
            .setDataType(
                TSDataType.getTsDataType(result.getDeviewViewFieldTypeMap().get(sourceName)));
      } else {
        return new TSStatus(TSStatusCode.TYPE_NOT_FOUND.getStatusCode())
            .setMessage(
                String.format(
                    ManagerMessages.MESSAGE_MEASUREMENTS_NOT_FOUND_ARG_CANNOT_AUTO_DETECT_980D7D44,
                    unknownField.getKey()));
      }
    }
    return StatusUtils.OK;
  }

  private TsTableColumnSchema toFieldColumnSchemaWithOriginName(
      final String field, final byte type) {
    final FieldColumnSchema columnSchema =
        new FieldColumnSchema(field, TSDataType.getTsDataType(type));
    if (!field.equals(lowerCase2OriginalMap.get(field))) {
      TreeViewSchema.setOriginalName(columnSchema, lowerCase2OriginalMap.get(field));
    }
    return columnSchema;
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getLatestSchemaRegionMap() {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(path);
    tree.constructTree();
    return configManager.getRelatedSchemaRegionGroup(tree);
  }

  private class TreeDeviceViewFieldDetectionTaskExecutor
      extends DataNodeRegionTaskExecutor<TDeviceViewReq, TDeviceViewResp> {

    /**
     * Whether several types of the same measurement are tolerated. When they are, the type the
     * detection meets first is kept instead of failing the whole detection.
     */
    private final boolean tolerateTypeMismatch;

    protected TreeDeviceViewFieldDetectionTaskExecutor(
        final ConfigManager configManager,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
        final int tagNumber,
        final boolean restrict,
        final @Nullable Set<String> measurements,
        final boolean tolerateTypeMismatch) {
      super(
          configManager,
          targetRegionGroup,
          false,
          CnToDnAsyncRequestType.DETECT_TREE_DEVICE_VIEW_FIELD_TYPE,
          ((dataNodeLocation, consensusGroupIdList) -> {
            final TDeviceViewReq req =
                new TDeviceViewReq(
                    consensusGroupIdList, Arrays.asList(path.getNodes()), tagNumber, restrict);
            if (Objects.nonNull(measurements)) {
              req.setRequiredMeasurements(measurements);
            }
            if (tolerateTypeMismatch) {
              req.setTolerateTypeMismatch(true);
            }
            return req;
          }));
      this.tolerateTypeMismatch = tolerateTypeMismatch;
    }

    @Override
    protected List<TConsensusGroupId> processResponseOfOneDataNode(
        final TDataNodeLocation dataNodeLocation,
        final List<TConsensusGroupId> consensusGroupIdList,
        TDeviceViewResp response) {
      // Fail-fast
      if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return Collections.emptyList();
      }

      final List<TConsensusGroupId> failedRegionList = new ArrayList<>();
      if (response.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        mergeDeviceViewResp(response);
        return Collections.emptyList();
      } else if (response.getStatus().getCode()
          == TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode()) {
        result = response;
        return Collections.emptyList();
      }

      if (response.getStatus().getCode() == TSStatusCode.MULTIPLE_ERROR.getStatusCode()) {
        final List<TSStatus> subStatus = response.getStatus().getSubStatus();
        for (int i = 0; i < subStatus.size(); i++) {
          if (subStatus.get(i).getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            mergeDeviceViewResp(response);
            response = null;
          } else if (subStatus.get(i).getCode()
              == TSStatusCode.DATA_TYPE_MISMATCH.getStatusCode()) {
            response.setStatus(subStatus.get(i));
            result = response;
            return Collections.emptyList();
          } else if (Objects.nonNull(response)) {
            failedRegionList.add(consensusGroupIdList.get(i));
          }
        }
      } else {
        failedRegionList.addAll(consensusGroupIdList);
      }
      return failedRegionList;
    }

    private void mergeDeviceViewResp(final TDeviceViewResp resp) {
      // The map is always nonnull in the resp
      resp.getDeviewViewFieldTypeMap().forEach(this::mergeMeasurementAndType);
    }

    private void mergeMeasurementAndType(final String measurement, final byte type) {
      final String fieldName = measurement.toLowerCase(Locale.ENGLISH);

      // Field type collection
      if (!result.getDeviewViewFieldTypeMap().containsKey(fieldName)) {
        result.getDeviewViewFieldTypeMap().put(fieldName, type);
      } else if (!Objects.equals(result.getDeviewViewFieldTypeMap().get(fieldName), type)
          && !tolerateTypeMismatch) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.DATA_TYPE_MISMATCH,
                String.format(
                    ManagerMessages
                        .MESSAGE_MULTIPLE_TYPES_ENCOUNTERED_WHEN_AUTO_DETECTING_TYPE_OF_MEASUREMENT_ARG_PLEASE_CHECK_6EDF48DB,
                    measurement)));
      }

      // Field name detection
      if (!lowerCase2OriginalMap.containsKey(fieldName)) {
        lowerCase2OriginalMap.put(fieldName, measurement);
      } else if (!Objects.equals(lowerCase2OriginalMap.get(fieldName), measurement)
          && !tolerateTypeMismatch) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.MEASUREMENT_NAME_CONFLICT,
                String.format(
                    ManagerMessages
                        .MESSAGE_THE_MEASUREMENTS_ARG_AND_ARG_SHARE_THE_SAME_LOWER_CASE_WHEN_AUTO_DETECTING_TYPE_PLEASE_CHECK_7225D062,
                    lowerCase2OriginalMap.get(fieldName),
                    measurement)));
      }
    }

    @Override
    protected void onAllReplicasetFailure(
        final TConsensusGroupId consensusGroupId,
        final Set<TDataNodeLocation> dataNodeLocationSet) {
      final String errorMsg = "Failed to get device view field type on region {}.";
      LOGGER.warn(errorMsg, consensusGroupId);
      result =
          new TDeviceViewResp(
              RpcUtils.getStatus(TSStatusCode.TYPE_NOT_FOUND, errorMsg), Collections.emptyMap());
      interruptTask();
    }
  }
}
