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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

public class TriggerExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerExecutor.class);

  private final TSDataType seriesDataType;
  private final TriggerRegistrationInformation registrationInformation;
  private final TriggerAttributes attributes;

  private TriggerClassLoader triggerClassLoader;

  private Trigger preparedTrigger;
  private Trigger committedTrigger;

  public TriggerExecutor(
      TSDataType seriesDataType,
      TriggerRegistrationInformation registrationInformation,
      TriggerClassLoader triggerClassLoader)
      throws TriggerManagementException {
    this.seriesDataType = seriesDataType;
    this.registrationInformation = registrationInformation;
    attributes = new TriggerAttributes(registrationInformation.getAttributes());

    this.triggerClassLoader = triggerClassLoader;

    preparedTrigger = null;
    committedTrigger = constructTriggerInstance(triggerClassLoader);
  }

  public void prepareTriggerUpdate(TriggerClassLoader newClassLoader)
      throws TriggerExecutionException, TriggerManagementException {
    preparedTrigger = constructTriggerInstance(newClassLoader);
    onStart(preparedTrigger, newClassLoader);
  }

  public void abortTriggerUpdate() {
    preparedTrigger = null;
  }

  /**
   * The execution of {@link TriggerExecutor#fire(long, Object)} should be blocked when the trigger
   * class is updating.
   */
  public synchronized void commitTriggerUpdate(TriggerClassLoader newClassLoader) {
    if (preparedTrigger == null) {
      LOGGER.warn(
          "Trigger {}({}) update is not prepared.",
          registrationInformation.getTriggerName(),
          registrationInformation.getClassName());
      return;
    }

    try {
      onStop(committedTrigger, triggerClassLoader);
    } catch (TriggerExecutionException e) {
      LOGGER.warn("Error occurred when updating trigger instance.", e);
    }

    triggerClassLoader = newClassLoader;

    committedTrigger = preparedTrigger;
    preparedTrigger = null;
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

  public void onStart() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    onStart(committedTrigger, triggerClassLoader);
    registrationInformation.markAsStarted();
  }

  private synchronized void onStart(Trigger trigger, TriggerClassLoader classLoader)
      throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onStart(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("onStart(TriggerAttributes)", e);
    }
  }

  public void onStop() throws TriggerExecutionException {
    // The execution order of statement here cannot be swapped!
    registrationInformation.markAsStopped();
    onStop(committedTrigger, triggerClassLoader);
  }

  private synchronized void onStop(Trigger trigger, TriggerClassLoader classLoader)
      throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(classLoader);

    try {
      trigger.onStop();
    } catch (Exception e) {
      onTriggerExecutionError("onStop()", e);
    }
  }

  public void fireIfActivated(long time, Object value) throws TriggerExecutionException {
    if (!registrationInformation.isStopped()) {
      fire(time, value);
    }
  }

  private synchronized void fire(long time, Object value) throws TriggerExecutionException {
    Thread.currentThread().setContextClassLoader(triggerClassLoader);

    try {
      switch (seriesDataType) {
        case INT32:
          committedTrigger.fire(time, (Integer) value);
          break;
        case INT64:
          committedTrigger.fire(time, (Long) value);
          break;
        case FLOAT:
          committedTrigger.fire(time, (Float) value);
          break;
        case DOUBLE:
          committedTrigger.fire(time, (Double) value);
          break;
        case BOOLEAN:
          committedTrigger.fire(time, (Boolean) value);
          break;
        case TEXT:
          committedTrigger.fire(time, (Binary) value);
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
}
