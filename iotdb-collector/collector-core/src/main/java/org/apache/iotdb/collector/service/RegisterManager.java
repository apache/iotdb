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

package org.apache.iotdb.collector.service;

import org.apache.iotdb.commons.exception.StartupException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RegisterManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterManager.class);
  private final List<IService> iServices;
  private static final long DEREGISTER_TIME_OUT = 10_00L;

  private RegisterManager() {
    this.iServices = new ArrayList<>();
  }

  /** register service. */
  public void register(final IService service) throws StartupException {
    for (final IService iService : iServices) {
      if (iService.getID() == service.getID()) {
        LOGGER.debug("{} has already been registered. skip", service.getID().getName());
        return;
      }
    }
    iServices.add(service);
    final long startTime = System.currentTimeMillis();
    service.start();
    final long endTime = System.currentTimeMillis();
    LOGGER.info(
        "The {} service is started successfully, which takes {} ms.",
        service.getID().getName(),
        (endTime - startTime));
  }

  /** stop all service and clear iService list. */
  public void deregisterAll() {
    Collections.reverse(iServices);
    for (IService service : iServices) {
      try {
        service.waitAndStop(DEREGISTER_TIME_OUT);
        LOGGER.debug("{} deregistered", service.getID());
      } catch (final Exception e) {
        LOGGER.error("Failed to stop {} because:", service.getID().getName(), e);
      }
    }
    iServices.clear();
    LOGGER.info("deregister all service.");
  }

  public static RegisterManager getInstance() {
    return RegisterManagerHolder.INSTANCE;
  }

  private static class RegisterManagerHolder {
    private static final RegisterManager INSTANCE = new RegisterManager();

    private RegisterManagerHolder() {}
  }
}
