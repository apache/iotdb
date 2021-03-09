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

package org.apache.iotdb.db.engine.trigger.executor;

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.db.engine.trigger.service.TriggerClassLoader;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationInformation;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

public class TriggerExecutor {

  private final TriggerRegistrationInformation registrationInformation;
  private final TriggerAttributes attributes;

  private final MeasurementMNode measurementMNode;
  private final TSDataType seriesDataType;

  private TriggerClassLoader triggerClassLoader;

  private Trigger preparedTrigger;
  private Trigger committedTrigger;

  public TriggerExecutor(
      TriggerRegistrationInformation registrationInformation,
      MeasurementMNode measurementMNode,
      TriggerClassLoader triggerClassLoader)
      throws TriggerManagementException {
    this.registrationInformation = registrationInformation;
    attributes = new TriggerAttributes(registrationInformation.getAttributes());

    this.measurementMNode = measurementMNode;
    seriesDataType = measurementMNode.getSchema().getType();

    this.triggerClassLoader = triggerClassLoader;

    preparedTrigger = null;
    committedTrigger = constructTriggerInstance(triggerClassLoader);
  }

  public void prepareTriggerUpdate(TriggerClassLoader newClassLoader)
      throws TriggerManagementException {
    preparedTrigger = constructTriggerInstance(newClassLoader);
  }

  public void abortTriggerUpdate() {
    preparedTrigger = null;
  }

  /**
   * The execution of {@link TriggerExecutor#fire(long, Object)} and {@link
   * TriggerExecutor#fire(long[], Object)} should be blocked when the trigger class is updating.
   */
  public synchronized void commitTriggerUpdate(TriggerClassLoader newClassLoader)
      throws TriggerExecutionException {
    try {
      Thread.currentThread().setContextClassLoader(triggerClassLoader);
      Map<String, Object> migratedObjects = committedTrigger.migrateToNew();

      Thread.currentThread().setContextClassLoader(newClassLoader);
      preparedTrigger.migrateFromOld(migratedObjects);
    } catch (Exception e) {
      registrationInformation.markAsStopped();
      throw new TriggerExecutionException(
          "Failed to migrate data from the old trigger instance to the new.", e);
    } finally {
      triggerClassLoader = newClassLoader;

      committedTrigger = preparedTrigger;
      preparedTrigger = null;
    }
  }

  private Trigger constructTriggerInstance(TriggerClassLoader classLoader)
      throws TriggerManagementException {
    try {
      Class<?> triggerClass =
          Class.forName(registrationInformation.getClassName(), true, classLoader);
      return (Trigger) triggerClass.getDeclaredConstructor().newInstance();
    } catch (InstantiationException
        | InvocationTargetException
        | NoSuchMethodException
        | IllegalAccessException
        | ClassNotFoundException e) {
      throw new TriggerManagementException(
          String.format(
              "Failed to reflect Trigger %s(%s) instance, because %s",
              registrationInformation.getTriggerName(), registrationInformation.getClassName(), e));
    }
  }

  public void onConfig() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      committedTrigger.onConfig(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("onConfig(TriggerAttributes)", e);
    }
  }

  public void onStart() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    invokeOnStart();
    registrationInformation.markAsStarted();
  }

  private synchronized void invokeOnStart() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      committedTrigger.onStart();
    } catch (Exception e) {
      onTriggerExecutionError("onStart()", e);
    }
  }

  public void onStop() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    registrationInformation.markAsStopped();
    invokeOnStop();
  }

  private synchronized void invokeOnStop() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      committedTrigger.onStop();
    } catch (Exception e) {
      onTriggerExecutionError("onStop()", e);
    }
  }

  public void fireIfActivated(long timestamp, Object value) throws TriggerExecutionException {
    if (!registrationInformation.isStopped()) {
      fire(timestamp, value);
    }
  }

  private synchronized void fire(long timestamp, Object value) throws TriggerExecutionException {
    // double check on purpose: the running status may be changed by the method commitTriggerUpdate.
    if (registrationInformation.isStopped()) {
      return;
    }

    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          committedTrigger.fire(timestamp, (Integer) value);
          break;
        case INT64:
          committedTrigger.fire(timestamp, (Long) value);
          break;
        case FLOAT:
          committedTrigger.fire(timestamp, (Float) value);
          break;
        case DOUBLE:
          committedTrigger.fire(timestamp, (Double) value);
          break;
        case BOOLEAN:
          committedTrigger.fire(timestamp, (Boolean) value);
          break;
        case TEXT:
          committedTrigger.fire(timestamp, (Binary) value);
          break;
        default:
          throw new TriggerExecutionException("Unsupported series data type.");
      }
    } catch (TriggerExecutionException e) {
      throw e;
    } catch (Exception e) {
      onTriggerExecutionError("fire(long, Object)", e);
    }
  }

  public void fireIfActivated(long[] timestamps, Object values) throws TriggerExecutionException {
    if (!registrationInformation.isStopped()) {
      fire(timestamps, values);
    }
  }

  private synchronized void fire(long[] timestamps, Object values)
      throws TriggerExecutionException {
    // double check on purpose: the running status may be changed by the method commitTriggerUpdate.
    if (registrationInformation.isStopped()) {
      return;
    }

    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          committedTrigger.fire(timestamps, (int[]) values);
          break;
        case INT64:
          committedTrigger.fire(timestamps, (long[]) values);
          break;
        case FLOAT:
          committedTrigger.fire(timestamps, (float[]) values);
          break;
        case DOUBLE:
          committedTrigger.fire(timestamps, (double[]) values);
          break;
        case BOOLEAN:
          committedTrigger.fire(timestamps, (boolean[]) values);
          break;
        case TEXT:
          committedTrigger.fire(timestamps, (Binary[]) values);
          break;
        default:
          throw new TriggerExecutionException("Unsupported series data type.");
      }
    } catch (TriggerExecutionException e) {
      throw e;
    } catch (Exception e) {
      onTriggerExecutionError("fire(long[], Object)", e);
    }
  }

  private void onTriggerExecutionError(String methodName, Exception e)
      throws TriggerExecutionException {
    throw new TriggerExecutionException(
        String.format(
                "Error occurred during executing Trigger#%s: %s",
                methodName, System.lineSeparator())
            + e);
  }

  public TriggerRegistrationInformation getRegistrationInformation() {
    return registrationInformation;
  }

  public MeasurementMNode getMeasurementMNode() {
    return measurementMNode;
  }

  @TestOnly
  public Trigger getTrigger() {
    return committedTrigger;
  }
}
