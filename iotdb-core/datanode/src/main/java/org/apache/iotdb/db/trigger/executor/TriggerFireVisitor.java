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

package org.apache.iotdb.db.trigger.executor;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.ConfigRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.queryengine.plan.Coordinator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.pipe.PipeEnrichedInsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.RelationalInsertTabletNode;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerReq;
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerResp;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.apache.thrift.TException;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TriggerFireVisitor extends PlanVisitor<TriggerFireResult, TriggerEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerFireVisitor.class);

  private static final IClientManager<ConfigRegionId, ConfigNodeClient> CONFIG_NODE_CLIENT_MANAGER =
      ConfigNodeClientManager.getInstance();

  /**
   * How many times should we retry when error occurred during firing a trigger on another datanode.
   */
  private static final int FIRE_RETRY_NUM =
      IoTDBDescriptor.getInstance().getConfig().getRetryNumToFindStatefulTrigger();

  @Override
  public TriggerFireResult process(PlanNode node, TriggerEvent context) {
    if (TriggerManagementService.getInstance().isTriggerTableEmpty()) {
      return TriggerFireResult.SUCCESS;
    }
    return node.accept(this, context);
  }

  @Override
  public TriggerFireResult visitPlan(PlanNode node, TriggerEvent context) {
    return TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitInsertRow(InsertRowNode node, TriggerEvent context) {
    Map<String, List<String>> triggerNameToMeasurementList =
        constructTriggerNameToMeasurementListMap(node, context);
    // return success if no trigger is found
    if (triggerNameToMeasurementList.isEmpty()) {
      return TriggerFireResult.SUCCESS;
    }

    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    Map<String, Integer> measurementToSchemaIndexMap =
        constructMeasurementToSchemaIndexMap(node.getMeasurements(), measurementSchemas);

    Object[] values = node.getValues();
    long time = node.getTime();
    boolean hasFailedTrigger = false;
    for (Map.Entry<String, List<String>> entry : triggerNameToMeasurementList.entrySet()) {
      List<IMeasurementSchema> schemas =
          entry.getValue().stream()
              .map(measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
              .collect(Collectors.toList());
      // only one row
      Tablet tablet = new Tablet(node.getTargetPath().getFullPath(), schemas, 1);
      // add one row
      tablet.addTimestamp(0, time);
      for (String measurement : entry.getValue()) {
        tablet.addValue(measurement, 0, values[measurementToSchemaIndexMap.get(measurement)]);
      }
      TriggerFireResult result = fire(entry.getKey(), tablet, context);
      // Terminate if a trigger with pessimistic strategy messes up
      if (result.equals(TriggerFireResult.TERMINATION)) {
        return result;
      }
      if (result.equals(TriggerFireResult.FAILED_NO_TERMINATION)) {
        hasFailedTrigger = true;
      }
    }
    return hasFailedTrigger ? TriggerFireResult.FAILED_NO_TERMINATION : TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitRelationalInsertTablet(
      RelationalInsertTabletNode node, TriggerEvent context) {
    // TODO-Table: add support
    return visitInsertTablet(node, context);
  }

  @Override
  public TriggerFireResult visitInsertTablet(InsertTabletNode node, TriggerEvent context) {
    // group Triggers and measurements
    Map<String, List<String>> triggerNameToMeasurementList =
        constructTriggerNameToMeasurementListMap(node, context);
    // return success if no trigger is found
    if (triggerNameToMeasurementList.isEmpty()) {
      return TriggerFireResult.SUCCESS;
    }

    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    Map<String, Integer> measurementToSchemaIndexMap =
        constructMeasurementToSchemaIndexMap(node.getMeasurements(), measurementSchemas);

    Object[] columns = node.getColumns();
    BitMap[] bitMaps = node.getBitMaps();
    long[] timestamps = node.getTimes();
    int rowCount = node.getRowCount();
    boolean hasFailedTrigger = false;
    for (Map.Entry<String, List<String>> entry : triggerNameToMeasurementList.entrySet()) {
      Tablet tablet;
      if (entry.getValue().size() == measurementSchemas.length) {
        // all measurements are included
        tablet =
            new Tablet(
                node.getTargetPath().getFullPath(),
                Arrays.asList(measurementSchemas),
                timestamps,
                columns,
                bitMaps,
                rowCount);
      } else {
        // choose specified columns
        List<IMeasurementSchema> schemas =
            entry.getValue().stream()
                .map(
                    measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
                .collect(Collectors.toList());
        Object[] columnsOfNewTablet =
            entry.getValue().stream()
                .map(measurement -> columns[measurementToSchemaIndexMap.get(measurement)])
                .toArray();
        BitMap[] bitMapsOfNewTablet = new BitMap[entry.getValue().size()];
        if (bitMaps != null) {
          for (int i = 0; i < entry.getValue().size(); i++) {
            bitMapsOfNewTablet[i] =
                bitMaps[measurementToSchemaIndexMap.get(entry.getValue().get(i))];
          }
        }
        tablet =
            new Tablet(
                node.getTargetPath().getFullPath(),
                schemas,
                timestamps,
                columnsOfNewTablet,
                bitMapsOfNewTablet,
                rowCount);
      }

      TriggerFireResult result = fire(entry.getKey(), tablet, context);
      // Terminate if a trigger with pessimistic strategy messes up
      if (result.equals(TriggerFireResult.TERMINATION)) {
        return result;
      }
      if (result.equals(TriggerFireResult.FAILED_NO_TERMINATION)) {
        hasFailedTrigger = true;
      }
    }
    return hasFailedTrigger ? TriggerFireResult.FAILED_NO_TERMINATION : TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitInsertRows(InsertRowsNode node, TriggerEvent context) {
    boolean hasFailedTrigger = false;
    for (InsertRowNode insertRowNode : node.getInsertRowNodeList()) {
      TriggerFireResult result = visitInsertRow(insertRowNode, context);
      if (result.equals(TriggerFireResult.TERMINATION)) {
        return result;
      }
      if (result.equals(TriggerFireResult.FAILED_NO_TERMINATION)) {
        hasFailedTrigger = true;
      }
    }
    return hasFailedTrigger ? TriggerFireResult.FAILED_NO_TERMINATION : TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitInsertMultiTablets(
      InsertMultiTabletsNode node, TriggerEvent context) {
    boolean hasFailedTrigger = false;
    for (InsertTabletNode insertTabletNode : node.getInsertTabletNodeList()) {
      TriggerFireResult result = visitInsertTablet(insertTabletNode, context);
      if (result.equals(TriggerFireResult.TERMINATION)) {
        return result;
      }
      if (result.equals(TriggerFireResult.FAILED_NO_TERMINATION)) {
        hasFailedTrigger = true;
      }
    }
    return hasFailedTrigger ? TriggerFireResult.FAILED_NO_TERMINATION : TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitInsertRowsOfOneDevice(
      InsertRowsOfOneDeviceNode node, TriggerEvent context) {
    boolean hasFailedTrigger = false;
    for (InsertRowNode insertRowNode : node.getInsertRowNodeList()) {
      TriggerFireResult result = visitInsertRow(insertRowNode, context);
      if (result.equals(TriggerFireResult.TERMINATION)) {
        return result;
      }
      if (result.equals(TriggerFireResult.FAILED_NO_TERMINATION)) {
        hasFailedTrigger = true;
      }
    }
    return hasFailedTrigger ? TriggerFireResult.FAILED_NO_TERMINATION : TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitPipeEnrichedInsertNode(
      PipeEnrichedInsertNode node, TriggerEvent context) {
    return node.getInsertNode().accept(this, context);
  }

  private Map<String, Integer> constructMeasurementToSchemaIndexMap(
      String[] measurements, MeasurementSchema[] schemas) {
    // The index of measurement and schema is the same now.
    // However, in case one day the order changes, we need to construct an index map.
    Map<String, Integer> indexMap = new HashMap<>();
    for (int i = 0, n = measurements.length; i < n; i++) {
      if (measurements[i] == null) {
        continue;
      }
      // It is the same now
      if (schemas[i] != null && schemas[i].getMeasurementName().equals(measurements[i])) {
        indexMap.put(measurements[i], i);
        continue;
      }
      for (int j = 0, m = schemas.length; j < m; j++) {
        if (schemas[j] != null && schemas[j].getMeasurementName().equals(measurements[i])) {
          indexMap.put(measurements[i], j);
          break;
        }
      }
    }
    return indexMap;
  }

  private Map<String, List<String>> constructTriggerNameToMeasurementListMap(
      InsertNode node, TriggerEvent event) {
    PartialPath device = node.getTargetPath();
    List<String> measurements = new ArrayList<>();
    for (String measurement : node.getMeasurements()) {
      if (measurement != null) {
        measurements.add(measurement);
      }
    }

    List<List<String>> triggerNameLists =
        TriggerManagementService.getInstance().getMatchedTriggerListForPath(device, measurements);
    boolean isAllEmpty = true;
    for (List<String> triggerNameList : triggerNameLists) {
      if (!triggerNameList.isEmpty()) {
        isAllEmpty = false;
        break;
      }
    }
    if (isAllEmpty) {
      return Collections.emptyMap();
    }

    Map<String, List<String>> triggerNameToPaths = new HashMap<>();
    TriggerTable triggerTable = TriggerManagementService.getInstance().getTriggerTable();
    for (int i = 0, n = measurements.size(); i < n; i++) {
      for (String triggerName : triggerNameLists.get(i)) {
        TriggerInformation triggerInformation = triggerTable.getTriggerInformation(triggerName);
        if (triggerInformation.getEvent().equals(event)
            && triggerInformation.getTriggerState().equals(TTriggerState.ACTIVE)) {
          triggerNameToPaths
              .computeIfAbsent(triggerName, k -> new ArrayList<>())
              .add(measurements.get(i));
        }
      }
    }
    return triggerNameToPaths;
  }

  private TriggerFireResult fire(String triggerName, Tablet tablet, TriggerEvent event) {
    TriggerFireResult result = TriggerFireResult.SUCCESS;
    for (int i = 0; i < FIRE_RETRY_NUM; i++) {
      if (TriggerManagementService.getInstance().needToFireOnAnotherDataNode(triggerName)) {
        TDataNodeLocation tDataNodeLocation =
            TriggerManagementService.getInstance()
                .getDataNodeLocationOfStatefulTrigger(triggerName);
        try (SyncDataNodeInternalServiceClient client =
            Coordinator.getInstance()
                .getInternalServiceClientManager()
                .borrowClient(tDataNodeLocation.getInternalEndPoint())) {
          TFireTriggerReq req = new TFireTriggerReq(triggerName, tablet.serialize(), event.getId());
          TFireTriggerResp resp = client.fireTrigger(req);
          if (resp.foundExecutor) {
            // we successfully found an executor on another data node
            return TriggerFireResult.construct(resp.getFireResult());
          } else {
            // update TDataNodeLocation of stateful trigger through config node
            if (!updateLocationOfStatefulTrigger(triggerName, tDataNodeLocation.getDataNodeId())) {
              // if TDataNodeLocation is still the same, sleep 1s and before the retry
              Thread.sleep(4000);
            }
          }
        } catch (ClientManagerException | TException e) {
          // IOException means that we failed to borrow client, possibly because corresponding
          // DataNode is down.
          // TException means there's a timeout or broken connection.
          // We need to update local TriggerTable with the new TDataNodeLocation of the stateful
          // trigger.
          LOGGER.warn(
              "Error occurred when trying to fire trigger({}) on TEndPoint: {}, the cause is: {}",
              triggerName,
              tDataNodeLocation.getInternalEndPoint(),
              e);
          // update TDataNodeLocation of stateful trigger through config node
          updateLocationOfStatefulTrigger(triggerName, tDataNodeLocation.getDataNodeId());
        } catch (InterruptedException e) {
          LOGGER.warn("{} interrupted when sleep", triggerName);
          Thread.currentThread().interrupt();
        } catch (Exception e) {
          LOGGER.warn(
              "Error occurred when trying to fire trigger({}) on TEndPoint: {}, the cause is: {}",
              triggerName,
              tDataNodeLocation.getInternalEndPoint(),
              e);
          // do not retry if it is not due to bad network or no executor found
          return TriggerManagementService.getInstance()
                  .getTriggerInformation(triggerName)
                  .getFailureStrategy()
                  .equals(FailureStrategy.OPTIMISTIC)
              ? TriggerFireResult.FAILED_NO_TERMINATION
              : TriggerFireResult.TERMINATION;
        }
      } else {
        TriggerExecutor executor = TriggerManagementService.getInstance().getExecutor(triggerName);
        if (executor == null) {
          return TriggerManagementService.getInstance()
                  .getTriggerInformation(triggerName)
                  .getFailureStrategy()
                  .equals(FailureStrategy.PESSIMISTIC)
              ? TriggerFireResult.TERMINATION
              : TriggerFireResult.FAILED_NO_TERMINATION;
        }
        try {
          boolean fireResult = executor.fire(tablet, event);
          if (!fireResult) {
            result =
                executor.getFailureStrategy().equals(FailureStrategy.PESSIMISTIC)
                    ? TriggerFireResult.TERMINATION
                    : TriggerFireResult.FAILED_NO_TERMINATION;
          }
        } catch (TriggerExecutionException e) {
          result =
              executor.getFailureStrategy().equals(FailureStrategy.PESSIMISTIC)
                  ? TriggerFireResult.TERMINATION
                  : TriggerFireResult.FAILED_NO_TERMINATION;
        }
        return result;
      }
    }
    return result;
  }

  /** Return true if the config node returns a new TDataNodeLocation. */
  private boolean updateLocationOfStatefulTrigger(String triggerName, int currentDataNodeId) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      TDataNodeLocation newTDataNodeLocation =
          configNodeClient.getLocationOfStatefulTrigger(triggerName).getDataNodeLocation();
      if (newTDataNodeLocation != null
          && currentDataNodeId != newTDataNodeLocation.getDataNodeId()) {
        // indicates that the location of this stateful trigger has changed
        TriggerManagementService.getInstance()
            .updateLocationOfStatefulTrigger(triggerName, newTDataNodeLocation);
        return true;
      }
      return false;
    } catch (ClientManagerException | TException | IOException e) {
      LOGGER.error(
          "Failed to update location of stateful trigger({}) through config node. The cause is {}.",
          triggerName,
          e);
      return false;
    }
  }
}
