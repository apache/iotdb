/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.service;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.exception.StartupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RegisterManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(RegisterManager.class);
  private List<IService> iServices;

  public RegisterManager() {
    iServices = new ArrayList<>();
  }

  /**
   * register service.
   */
  public void register(IService service) throws StartupException {
    for (IService s : iServices) {
      if (s.getID() == service.getID()) {
        LOGGER.info("{} has already been registered. skip", service.getID().getName());
        return;
      }
    }
    iServices.add(service);
    service.start();
  }

  /**
   * stop all service and clear iService list.
   */
  public void deregisterAll() {
    for (IService service : iServices) {
      try {
        service.stop();
      } catch (Exception e) {
        LOGGER.error("Failed to stop {} because {}", service.getID().getName(), e.getMessage());
      }
    }
    iServices.clear();
    LOGGER.info("deregister all service.");
  }
}
