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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.commons.schema.table.TreeViewSchema;
import org.apache.iotdb.commons.schema.table.TsTable;
import org.apache.iotdb.commons.schema.table.column.FieldColumnSchema;
import org.apache.iotdb.commons.schema.table.column.TsTableColumnSchema;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.procedure.impl.schema.DataNodeRegionTaskExecutor;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewReq;
import org.apache.iotdb.mpp.rpc.thrift.TDeviceViewResp;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
  private final Map<String, Set<FieldColumnSchema>> fields;

  private TDeviceViewResp result = new TDeviceViewResp(StatusUtils.OK, new ConcurrentHashMap<>());
  private final Map<String, String> lowerCase2OriginalMap = new HashMap<>();

  public TreeDeviceViewFieldDetector(
      final ConfigManager configManager,
      final TsTable table,
      final Map<String, Set<FieldColumnSchema>> fields) {
    this.configManager = configManager;
    this.path = TreeViewSchema.getPrefixPattern(table);
    this.table = table;
    this.fields = fields;
  }

  public TSStatus detectMissingFieldTypes() {
    if (table.getFieldNum() == 0 && Objects.isNull(fields)) {
      new TreeDeviceViewFieldDetectionTaskExecutor(
              configManager,
              getLatestSchemaRegionMap(),
              table.getTagNum(),
              TreeViewSchema.isRestrict(table))
          .execute();
      if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result.getStatus();
      }
      result
          .getDeviewViewFieldTypeMap()
          .forEach(
              (field, type) -> {
                final FieldColumnSchema columnSchema =
                    new FieldColumnSchema(field, TSDataType.getTsDataType(type));
                if (!field.equals(lowerCase2OriginalMap.get(field))) {
                  TreeViewSchema.setOriginalName(columnSchema, lowerCase2OriginalMap.get(field));
                }
                table.addColumnSchema(columnSchema);
              });
    } else {
      final Map<String, Set<FieldColumnSchema>> unknownFields;
      if (Objects.isNull(fields)) {
        unknownFields = new HashMap<>();
        for (final TsTableColumnSchema schema : table.getColumnList()) {
          if (!(schema instanceof FieldColumnSchema)
              || schema.getDataType() != TSDataType.UNKNOWN) {
            continue;
          }
          final String key = TreeViewSchema.getSourceName(schema);
          if (!unknownFields.containsKey(key)) {
            unknownFields.put(key, new HashSet<>());
          }
          unknownFields.get(key).add((FieldColumnSchema) schema);
        }
      } else {
        unknownFields = fields;
      }

      if (unknownFields.isEmpty()) {
        return StatusUtils.OK;
      }
      new TreeDeviceViewFieldDetectionTaskExecutor(
              configManager,
              getLatestSchemaRegionMap(),
              table.getTagNum(),
              TreeViewSchema.isRestrict(table),
              unknownFields.keySet())
          .execute();
      if (result.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result.getStatus();
      }
      for (final Map.Entry<String, Set<FieldColumnSchema>> unknownField :
          unknownFields.entrySet()) {
        if (result.getDeviewViewFieldTypeMap().containsKey(unknownField.getKey())) {
          unknownField
              .getValue()
              .forEach(
                  field ->
                      field.setDataType(
                          TSDataType.getTsDataType(
                              result.getDeviewViewFieldTypeMap().get(unknownField.getKey()))));
        } else {
          return new TSStatus(TSStatusCode.TYPE_NOT_FOUND.getStatusCode())
              .setMessage(
                  String.format(
                      "Measurements not found for %s, cannot auto detect", unknownField.getKey()));
        }
      }
    }
    return StatusUtils.OK;
  }

  private Map<TConsensusGroupId, TRegionReplicaSet> getLatestSchemaRegionMap() {
    final PathPatternTree tree = new PathPatternTree();
    tree.appendPathPattern(path);
    tree.constructTree();
    return configManager.getRelatedSchemaRegionGroup(tree);
  }

  private class TreeDeviceViewFieldDetectionTaskExecutor
      extends DataNodeRegionTaskExecutor<TDeviceViewReq, TDeviceViewResp> {

    protected TreeDeviceViewFieldDetectionTaskExecutor(
        final ConfigManager configManager,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
        final int tagNumber,
        final boolean restrict) {
      super(
          configManager,
          targetRegionGroup,
          false,
          CnToDnAsyncRequestType.DETECT_TREE_DEVICE_VIEW_FIELD_TYPE,
          ((dataNodeLocation, consensusGroupIdList) ->
              new TDeviceViewReq(
                  consensusGroupIdList, Arrays.asList(path.getNodes()), tagNumber, restrict)));
      configManager
          .getClusterSchemaManager()
          .getTemplateSetInfo(Collections.singletonList(path))
          .getPatternTemplateMap()
          .values()
          .stream()
          .flatMap(Collection::stream)
          .flatMap(template -> template.getSchemaMap().values().stream())
          .forEach(
              schema ->
                  mergeMeasurementAndType(schema.getMeasurementName(), schema.getTypeInByte()));
    }

    protected TreeDeviceViewFieldDetectionTaskExecutor(
        final ConfigManager configManager,
        final Map<TConsensusGroupId, TRegionReplicaSet> targetRegionGroup,
        final int tagNumber,
        final boolean restrict,
        final @Nonnull Set<String> measurements) {
      super(
          configManager,
          targetRegionGroup,
          false,
          CnToDnAsyncRequestType.DETECT_TREE_DEVICE_VIEW_FIELD_TYPE,
          ((dataNodeLocation, consensusGroupIdList) ->
              new TDeviceViewReq(
                      consensusGroupIdList, Arrays.asList(path.getNodes()), tagNumber, restrict)
                  .setRequiredMeasurements(measurements)));
      configManager
          .getClusterSchemaManager()
          .getTemplateSetInfo(Collections.singletonList(path))
          .getPatternTemplateMap()
          .values()
          .stream()
          .flatMap(Collection::stream)
          .flatMap(template -> template.getSchemaMap().values().stream())
          .filter(schema -> measurements.contains(schema.getMeasurementName()))
          .forEach(
              schema ->
                  mergeMeasurementAndType(schema.getMeasurementName(), schema.getTypeInByte()));
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
      } else if (!Objects.equals(result.getDeviewViewFieldTypeMap().get(fieldName), type)) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.DATA_TYPE_MISMATCH,
                String.format(
                    "Multiple types encountered when auto detecting type of measurement '%s', please check",
                    measurement)));
      }

      // Field name detection
      if (!lowerCase2OriginalMap.containsKey(fieldName)) {
        lowerCase2OriginalMap.put(fieldName, measurement);
      } else if (!Objects.equals(lowerCase2OriginalMap.get(fieldName), measurement)) {
        result.setStatus(
            RpcUtils.getStatus(
                TSStatusCode.MEASUREMENT_NAME_CONFLICT,
                String.format(
                    "The measurements %s and %s share the same lower case when auto detecting type, please check",
                    lowerCase2OriginalMap.get(fieldName), measurement)));
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
