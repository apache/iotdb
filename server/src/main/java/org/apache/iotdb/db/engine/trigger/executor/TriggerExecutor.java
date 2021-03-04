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

import java.lang.reflect.InvocationTargetException;

public class TriggerExecutor {

  private final TSDataType seriesDataType;
  private final TriggerRegistrationInformation registrationInformation;
  private final TriggerAttributes attributes;

  private Trigger trigger;

  public TriggerExecutor(
      TSDataType seriesDataType,
      TriggerRegistrationInformation registrationInformation,
      TriggerClassLoader classLoader)
      throws TriggerManagementException {
    this.seriesDataType = seriesDataType;
    this.registrationInformation = registrationInformation;
    attributes = new TriggerAttributes(registrationInformation.getAttributes());
    constructTriggerInstance(classLoader);
  }

  public synchronized void updateClass(TriggerClassLoader classLoader)
      throws TriggerExecutionException, TriggerManagementException {
    onStop();
    constructTriggerInstance(classLoader);
    onStart();
  }

  /** Note: make sure that the classloader of current thread has been set to {@code classLoader}. */
  private void constructTriggerInstance(TriggerClassLoader classLoader)
      throws TriggerManagementException {
    try {
      Class<?> triggerClass =
          Class.forName(registrationInformation.getClassName(), true, classLoader);
      trigger = (Trigger) triggerClass.getDeclaredConstructor().newInstance();
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
    try {
      trigger.onStart(attributes);
    } catch (Exception e) {
      onTriggerExecutionError("onStart(TriggerAttributes)", e);
    }
  }

  public void onStop() throws TriggerExecutionException {
    try {
      trigger.onStop();
    } catch (Exception e) {
      onTriggerExecutionError("onStop()", e);
    }
  }

  public synchronized void fire(long time, Object value) throws TriggerExecutionException {
    try {
      switch (seriesDataType) {
        case INT32:
          trigger.fire(time, (Integer) value);
          break;
        case INT64:
          trigger.fire(time, (Long) value);
          break;
        case FLOAT:
          trigger.fire(time, (Float) value);
          break;
        case DOUBLE:
          trigger.fire(time, (Double) value);
          break;
        case BOOLEAN:
          trigger.fire(time, (Boolean) value);
          break;
        case TEXT:
          trigger.fire(time, (Binary) value);
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
}
