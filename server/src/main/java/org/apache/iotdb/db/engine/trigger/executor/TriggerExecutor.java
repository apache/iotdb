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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.service.TriggerClassLoader;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.service.TriggerRegistrationInformation;
import org.apache.iotdb.db.exception.TriggerExecutionException;
import org.apache.iotdb.db.exception.TriggerManagementException;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import java.lang.reflect.InvocationTargetException;

public class TriggerExecutor {

  private final TriggerRegistrationInformation registrationInformation;
  private final TriggerAttributes attributes;

  private final TriggerClassLoader classLoader;

  private final IMNode imNode;

  private final Trigger trigger;

  public TriggerExecutor(
      TriggerRegistrationInformation registrationInformation,
      TriggerClassLoader classLoader,
      IMNode imNode)
      throws TriggerManagementException {
    this.registrationInformation = registrationInformation;
    attributes = new TriggerAttributes(registrationInformation.getAttributes());

    this.classLoader = classLoader;

    this.imNode = imNode;

    trigger = constructTriggerInstance();
  }

  private Trigger constructTriggerInstance() throws TriggerManagementException {
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

  public void onCreate() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onCreate(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("onConfig(TriggerAttributes)", e);
    }

    // The field isStopped in the registrationInformation is volatile, so the method
    // registrationInformation.markAsStarted() is always invoked after the method
    // trigger.onCreate(attributes) is invoked. It guarantees that the trigger will not be triggered
    // before trigger.onCreate(attributes) is called.
    registrationInformation.markAsStarted();
  }

  public synchronized void onDrop() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    registrationInformation.markAsStopped();

    try {
      trigger.onDrop();
    } catch (Exception e) {
      onTriggerExecutionError("onConfig(TriggerAttributes)", e);
    }
  }

  public synchronized void onStart() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    invokeOnStart();
    registrationInformation.markAsStarted();
  }

  private void invokeOnStart() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onStart();
    } catch (Exception e) {
      onTriggerExecutionError("onStart()", e);
    }
  }

  public synchronized void onStop() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    registrationInformation.markAsStopped();
    invokeOnStop();
  }

  private void invokeOnStop() throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onStop();
    } catch (Exception e) {
      onTriggerExecutionError("onStop()", e);
    }
  }

  public void fireIfActivated(
      TriggerEvent event, long timestamp, Object value, TSDataType seriesDataType, PartialPath path)
      throws TriggerExecutionException {
    if (!registrationInformation.isStopped() && event.equals(registrationInformation.getEvent())) {
      fire(timestamp, value, seriesDataType, path);
    }
  }

  private synchronized void fire(
      long timestamp, Object value, TSDataType seriesDataType, PartialPath path)
      throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          trigger.fire(timestamp, (Integer) value, path);
          break;
        case INT64:
          trigger.fire(timestamp, (Long) value, path);
          break;
        case FLOAT:
          trigger.fire(timestamp, (Float) value, path);
          break;
        case DOUBLE:
          trigger.fire(timestamp, (Double) value, path);
          break;
        case BOOLEAN:
          trigger.fire(timestamp, (Boolean) value, path);
          break;
        case TEXT:
          trigger.fire(timestamp, (Binary) value, path);
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

  public void fireIfActivated(
      TriggerEvent event,
      long[] timestamps,
      Object values,
      TSDataType seriesDataType,
      PartialPath path)
      throws TriggerExecutionException {
    if (!registrationInformation.isStopped() && event.equals(registrationInformation.getEvent())) {
      fire(timestamps, values, seriesDataType, path);
    }
  }

  private synchronized void fire(
      long[] timestamps, Object values, TSDataType seriesDataType, PartialPath path)
      throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          trigger.fire(timestamps, (int[]) values, path);
          break;
        case INT64:
          trigger.fire(timestamps, (long[]) values, path);
          break;
        case FLOAT:
          trigger.fire(timestamps, (float[]) values, path);
          break;
        case DOUBLE:
          trigger.fire(timestamps, (double[]) values, path);
          break;
        case BOOLEAN:
          trigger.fire(timestamps, (boolean[]) values, path);
          break;
        case TEXT:
          trigger.fire(timestamps, (Binary[]) values, path);
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

  public IMNode getIMNode() {
    return imNode;
  }

  @TestOnly
  public Trigger getTrigger() {
    return trigger;
  }
}
