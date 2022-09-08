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

package org.apache.iotdb.commons.trigger.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class TriggerRegistrationService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerRegistrationService.class);

  private final ReentrantLock registrationLock;

  private TriggerRegistrationService() {
    this.registrationLock = new ReentrantLock();
  }

  public void acquireRegistrationLock() {
    registrationLock.lock();
  }

  public void releaseRegistrationLock() {
    registrationLock.unlock();
  }

  // todo: implementation
  public void register() {
    // validate before registering
    // add to triggerTable and set inactive
    // throw exception if registered
  };

  public void activeTrigger(String triggerName) {
    // active trigger in table
  };

  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static TriggerRegistrationService INSTANCE = null;

  public static synchronized TriggerRegistrationService setupAndGetInstance() {
    if (INSTANCE == null) {
      INSTANCE = new TriggerRegistrationService();
    }
    return INSTANCE;
  }

  public static TriggerRegistrationService getInstance() {
    return INSTANCE;
  }
}
