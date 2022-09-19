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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanVisitor;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertMultiTabletsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertRowsOfOneDeviceNode;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.write.InsertTabletNode;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerReq;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TriggerFireVisitor extends PlanVisitor<TriggerFireResult, TriggerEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerFireVisitor.class);

  private final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      internalServiceClientIClientManager =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  @Override
  public TriggerFireResult visitPlan(PlanNode node, TriggerEvent context) {
    return TriggerFireResult.SUCCESS;
  }

  @Override
  public TriggerFireResult visitInsertRow(InsertRowNode node, TriggerEvent context) {
    String device = node.getDevicePath().getFullPath();
    String[] measurements = node.getMeasurements();
    Map<String, List<String>> triggerNameToMeasurementList =
        constructTriggerNameToMeasurementListMap(node);
    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    // return success if no trigger is found
    if (triggerNameToMeasurementList.isEmpty()) {
      return TriggerFireResult.SUCCESS;
    }
    Map<String, Integer> measurementToSchemaIndexMap =
        constructMeasurementToSchemaIndexMap(measurements, measurementSchemas);

    Object[] values = node.getValues();
    long time = node.getTime();
    boolean hasFailedTrigger = false;
    for (Map.Entry<String, List<String>> entry : triggerNameToMeasurementList.entrySet()) {
      List<MeasurementSchema> schemas =
          entry.getValue().stream()
              .map(measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
              .collect(Collectors.toList());
      Tablet tablet = new Tablet(device, schemas);
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
  public TriggerFireResult visitInsertTablet(InsertTabletNode node, TriggerEvent context) {
    String device = node.getDevicePath().getFullPath();
    String[] measurements = node.getMeasurements();
    MeasurementSchema[] measurementSchemas = node.getMeasurementSchemas();
    // group Triggers and measurements
    Map<String, List<String>> triggerNameToMeasurementList =
        constructTriggerNameToMeasurementListMap(node);
    // return success if no trigger is found
    if (triggerNameToMeasurementList.isEmpty()) {
      return TriggerFireResult.SUCCESS;
    }
    Map<String, Integer> measurementToSchemaIndexMap =
        constructMeasurementToSchemaIndexMap(measurements, measurementSchemas);

    Object[] columns = node.getColumns();
    BitMap[] bitMaps = node.getBitMaps();
    long[] timestamps = node.getTimes();
    boolean hasFailedTrigger = false;
    for (Map.Entry<String, List<String>> entry : triggerNameToMeasurementList.entrySet()) {
      List<MeasurementSchema> schemas =
          entry.getValue().stream()
              .map(measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
              .collect(Collectors.toList());
      Object[] columnsOfNewTablet =
          entry.getValue().stream()
              .map(measurement -> columns[measurementToSchemaIndexMap.get(measurement)])
              .toArray();
      BitMap[] bitMapsOfNewTablet =
          entry.getValue().stream()
              .map(measurement -> bitMaps[measurementToSchemaIndexMap.get(measurement)])
              .toArray(BitMap[]::new);
      Tablet tablet =
          new Tablet(
              device,
              schemas,
              timestamps,
              columnsOfNewTablet,
              bitMapsOfNewTablet,
              timestamps.length);
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
      if (schemas[i] != null && schemas[i].getMeasurementId().equals(measurements[i])) {
        indexMap.put(measurements[i], i);
        continue;
      }
      for (int j = 0, m = schemas.length; j < m; j++) {
        if (schemas[j] != null && schemas[j].getMeasurementId().equals(measurements[i])) {
          indexMap.put(measurements[i], j);
          break;
        }
      }
    }
    return indexMap;
  }

  private Map<String, List<String>> constructTriggerNameToMeasurementListMap(InsertNode node) {
    PartialPath device = node.getDevicePath();
    String[] measurements = node.getMeasurements();
    Map<String, List<String>> triggerNameToPaths = new HashMap<>();
    for (String measurement : measurements) {
      if (measurement != null) {
        List<String> triggerList = getMatchedTriggerListForPath(device.concatNode(measurement));
        for (String trigger : triggerList) {
          triggerNameToPaths.computeIfAbsent(trigger, k -> new ArrayList<>()).add(measurement);
        }
      }
    }
    return triggerNameToPaths;
  }

  /**
   * @param fullPath PathPattern
   * @return all the triggers that matched this Pattern
   */
  private List<String> getMatchedTriggerListForPath(PartialPath fullPath) {
    return TriggerManagementService.getInstance().getMatchedTriggerListForPath(fullPath);
  }

  private TriggerFireResult fire(String triggerName, Tablet tablet, TriggerEvent event) {
    TriggerFireResult result = TriggerFireResult.SUCCESS;
    if (TriggerManagementService.getInstance().needToFireOnAnotherDataNode(triggerName)) {
      TEndPoint endPoint =
          TriggerManagementService.getInstance().getEndPointForStatefulTrigger(triggerName);
      try (SyncDataNodeInternalServiceClient client =
          internalServiceClientIClientManager.borrowClient(endPoint)) {
        TFireTriggerReq req = new TFireTriggerReq(triggerName, tablet.serialize(), event.getId());
        result = TriggerFireResult.construct(client.fireTrigger(req).getFireResult());
      } catch (IOException | TException e) {
        LOGGER.warn(
            "Error occurred when trying to fire trigger({}) on TEndPoint: {}, the cause is: {}",
            triggerName,
            endPoint.toString(),
            e.getMessage());
        result =
            TriggerManagementService.getInstance()
                    .getTriggerInformation(triggerName)
                    .getFailureStrategy()
                    .equals(FailureStrategy.OPTIMISTIC)
                ? TriggerFireResult.FAILED_NO_TERMINATION
                : TriggerFireResult.TERMINATION;
      }
    } else {
      TriggerExecutor executor = TriggerManagementService.getInstance().getExecutor(triggerName);
      try {
        executor.fire(tablet, event);
      } catch (TriggerExecutionException e) {
        if (executor.getFailureStrategy().equals(FailureStrategy.PESSIMISTIC)) {
          result = TriggerFireResult.TERMINATION;
        } else {
          result = TriggerFireResult.FAILED_NO_TERMINATION;
        }
      }
    }
    return result;
  }
}
