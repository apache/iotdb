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
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.consensus.PartitionRegionId;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerTable;
import org.apache.iotdb.commons.trigger.exception.TriggerExecutionException;
import org.apache.iotdb.db.client.ConfigNodeClient;
import org.apache.iotdb.db.client.ConfigNodeInfo;
import org.apache.iotdb.db.client.DataNodeClientPoolFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
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
import org.apache.iotdb.mpp.rpc.thrift.TFireTriggerResp;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TriggerFireVisitor extends PlanVisitor<TriggerFireResult, TriggerEvent> {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerFireVisitor.class);

  private static final IClientManager<TEndPoint, SyncDataNodeInternalServiceClient>
      INTERNAL_SERVICE_CLIENT_MANAGER =
          new IClientManager.Factory<TEndPoint, SyncDataNodeInternalServiceClient>()
              .createClientManager(
                  new DataNodeClientPoolFactory.SyncDataNodeInternalServiceClientPoolFactory());

  private static final IClientManager<PartitionRegionId, ConfigNodeClient>
      CONFIG_NODE_CLIENT_MANAGER =
          new IClientManager.Factory<PartitionRegionId, ConfigNodeClient>()
              .createClientManager(new DataNodeClientPoolFactory.ConfigNodeClientPoolFactory());

  /**
   * How many times should we retry when error occurred during firing a trigger on another datanode
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
      List<MeasurementSchema> schemas =
          entry.getValue().stream()
              .map(measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
              .collect(Collectors.toList());
      Tablet tablet = new Tablet(node.getDevicePath().getFullPath(), schemas);
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
    boolean hasFailedTrigger = false;
    for (Map.Entry<String, List<String>> entry : triggerNameToMeasurementList.entrySet()) {
      Tablet tablet;
      if (entry.getValue().size() == measurementSchemas.length) {
        // all measurements are included
        tablet =
            new Tablet(
                node.getDevicePath().getFullPath(),
                Arrays.asList(measurementSchemas),
                timestamps,
                columns,
                bitMaps,
                timestamps.length);
      } else {
        // choose specified columns
        List<MeasurementSchema> schemas =
            entry.getValue().stream()
                .map(
                    measurement -> measurementSchemas[measurementToSchemaIndexMap.get(measurement)])
                .collect(Collectors.toList());
        Object[] columnsOfNewTablet =
            entry.getValue().stream()
                .map(measurement -> columns[measurementToSchemaIndexMap.get(measurement)])
                .toArray();
        BitMap[] bitMapsOfNewTablet =
            entry.getValue().stream()
                .map(measurement -> bitMaps[measurementToSchemaIndexMap.get(measurement)])
                .toArray(BitMap[]::new);
        tablet =
            new Tablet(
                node.getDevicePath().getFullPath(),
                schemas,
                timestamps,
                columnsOfNewTablet,
                bitMapsOfNewTablet,
                timestamps.length);
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

  private Map<String, List<String>> constructTriggerNameToMeasurementListMap(
      InsertNode node, TriggerEvent event) {
    PartialPath device = node.getDevicePath();
    if (!TriggerManagementService.getInstance().hasTriggerUnderDevice(device)) {
      return Collections.emptyMap();
    }
    Map<String, List<String>> triggerNameToPaths = new HashMap<>();
    TriggerTable triggerTable = TriggerManagementService.getInstance().getTriggerTable();
    String[] measurements = node.getMeasurements();
    for (String measurement : measurements) {
      if (measurement != null) {
        List<String> triggerList = getMatchedTriggerListForPath(device.concatNode(measurement));
        for (String trigger : triggerList) {
          if (triggerTable.getTriggerInformation(trigger).getEvent().equals(event)) {
            triggerNameToPaths.computeIfAbsent(trigger, k -> new ArrayList<>()).add(measurement);
          }
        }
      }
    }
    return triggerNameToPaths;
  }

  private List<String> getMatchedTriggerListForPath(PartialPath fullPath) {
    return TriggerManagementService.getInstance().getMatchedTriggerListForPath(fullPath);
  }

  private TriggerFireResult fire(String triggerName, Tablet tablet, TriggerEvent event) {
    TriggerFireResult result = TriggerFireResult.SUCCESS;
    for (int i = 0; i < FIRE_RETRY_NUM; i++) {
      if (TriggerManagementService.getInstance().needToFireOnAnotherDataNode(triggerName)) {
        TEndPoint endPoint =
            TriggerManagementService.getInstance().getEndPointForStatefulTrigger(triggerName);
        try (SyncDataNodeInternalServiceClient client =
            INTERNAL_SERVICE_CLIENT_MANAGER.borrowClient(endPoint)) {
          TFireTriggerReq req = new TFireTriggerReq(triggerName, tablet.serialize(), event.getId());
          TFireTriggerResp resp = client.fireTrigger(req);
          if (resp.foundExecutor) {
            // we successfully found an executor on another data node
            return TriggerFireResult.construct(resp.getFireResult());
          } else {
            // update TDataNodeLocation of stateful trigger through config node
            if (!updateLocationOfStatefulTrigger(triggerName)) {
              // if TDataNodeLocation is still the same, sleep 1s and before the retry
              Thread.sleep(1000);
            }
          }
        } catch (IOException | TException e) {
          // IOException means that we failed to borrow client, possibly because corresponding
          // DataNode is down.
          // TException means there's a timeout or broken connection.
          // We need to update local TriggerTable with the new TDataNodeLocation of the stateful
          // trigger.
          LOGGER.warn(
              "Error occurred when trying to fire trigger({}) on TEndPoint: {}, the cause is: {}",
              triggerName,
              endPoint.toString(),
              e);
          // update TDataNodeLocation of stateful trigger through config node
          updateLocationOfStatefulTrigger(triggerName);
        } catch (Throwable e) {
          LOGGER.warn(
              "Error occurred when trying to fire trigger({}) on TEndPoint: {}, the cause is: {}",
              triggerName,
              endPoint.toString(),
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

  /** Return true if the config node returns a new TDataNodeLocation */
  private boolean updateLocationOfStatefulTrigger(String triggerName) {
    try (ConfigNodeClient configNodeClient =
        CONFIG_NODE_CLIENT_MANAGER.borrowClient(ConfigNodeInfo.partitionRegionId)) {
      TDataNodeLocation tDataNodeLocation =
          configNodeClient.getLocationOfStatefulTrigger(triggerName).getDataNodeLocation();
      if (tDataNodeLocation != null) {
        TriggerManagementService.getInstance()
            .updateLocationOfStatefulTrigger(triggerName, tDataNodeLocation);
        return TriggerManagementService.getInstance()
                .getTriggerInformation(triggerName)
                .getDataNodeLocation()
                .getDataNodeId()
            == tDataNodeLocation.getDataNodeId();
      } else {
        return false;
      }
    } catch (TException | IOException e) {
      LOGGER.error(
          "Failed to update location of stateful trigger({}) through config node and the cause is {}.",
          triggerName,
          e);
      return false;
    }
  }
}
