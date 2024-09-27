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

package org.apache.iotdb.db.schemaengine.schemaregion.attribute.update;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class GeneralRegionAttributeSecurityService {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(GeneralRegionAttributeSecurityService.class);

  private final ExecutorService securityServiceExecutor =
      IoTDBThreadPoolFactory.newSingleThreadExecutor(
          ThreadName.GENERAL_REGION_ATTRIBUTE_SECURITY_SERVICE.getName());

  private Future<?> executorFuture;
  private final Set<SchemaRegionId> regionLeaders = new HashSet<>();

  public void startBroadcast(final SchemaRegionId id) {
    if (regionLeaders.isEmpty()) {
      executorFuture = securityServiceExecutor.submit(this::execute);
      LOGGER.info("General region attribute security service is started successfully.");
    }

    regionLeaders.add(id);
  }

  public void stopBroadcast(final SchemaRegionId id) {
    regionLeaders.remove(id);

    if (regionLeaders.isEmpty()) {
      executorFuture.cancel(false);
      executorFuture = null;
      LOGGER.info("General region attribute security service is stopped successfully.");
    }
  }

  private synchronized void execute() {

    try {
      wait(
          IoTDBDescriptor.getInstance()
              .getConfig()
              .getGeneralRegionAttributeSecurityServiceIntervalSeconds());
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted when waiting for the next attribute broadcasting: {}", e.getMessage());
    } finally {
      securityServiceExecutor.submit(this::execute);
    }
  }

  /////////////////////////////// SingleTon ///////////////////////////////

  private GeneralRegionAttributeSecurityService() {
    // Do nothing
  }

  private static final class GeneralRegionAttributeSecurityServiceHolder {
    private static final GeneralRegionAttributeSecurityService INSTANCE =
        new GeneralRegionAttributeSecurityService();

    private GeneralRegionAttributeSecurityServiceHolder() {}
  }

  public static GeneralRegionAttributeSecurityService getInstance() {
    return GeneralRegionAttributeSecurityServiceHolder.INSTANCE;
  }
}
