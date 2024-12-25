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

package org.apache.iotdb.trigger;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.trigger.api.Trigger;
import org.apache.iotdb.trigger.api.TriggerAttributes;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class StatisticsUpdaterTrigger implements Trigger {

  private static final Logger LOGGER = LoggerFactory.getLogger(StatisticsUpdaterTrigger.class);

  private static final String TARGET_DEVICE = "root.__system.statistics";

  private static final String TARGET_SERIES = "total_count";
  private String ip;

  private int port;

  private Session session;

  private AtomicLong cnt = new AtomicLong(0);

  private Future<?> updateFuture;

  private final ScheduledExecutorService triggerInformationUpdateExecutor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          "Stateful-Trigger-Statistics-Updater");

  private static final long UPDATE_INTERVAL = 1000 * 20L;

  @Override
  public void onCreate(TriggerAttributes attributes) throws Exception {
    if (attributes.hasAttribute("ip")) {
      ip = attributes.getString("ip");
    } else {
      throw new IllegalArgumentException("ip is required");
    }
    if (attributes.hasAttribute("port")) {
      port = Integer.parseInt(attributes.getString("port"));
    } else {
      throw new IllegalArgumentException("port is required");
    }
  }

  @Override
  public boolean fire(Tablet tablet) throws Exception {
    ensureSession();
    if (tablet.bitMaps == null) {
      cnt.addAndGet((long) tablet.getRowSize() * tablet.getSchemas().size());
      return true;
    }
    for (int column = 0; column < tablet.getSchemas().size(); column++) {
      BitMap bitMap = tablet.bitMaps[column];
      if (bitMap == null) {
        cnt.addAndGet(tablet.getRowSize());
      } else {
        for (int row = 0; row < tablet.getRowSize(); row++) {
          if (!bitMap.isMarked(row)) {
            cnt.incrementAndGet();
          }
        }
      }
    }
    return true;
  }

  @Override
  public void restore() throws Exception {
    ensureSession();
    try {
      SessionDataSet sessionDataSet =
          session.executeQueryStatement(
              String.format("select last %s from %s", TARGET_SERIES, TARGET_DEVICE));
      if (sessionDataSet.hasNext()) {
        cnt = new AtomicLong(sessionDataSet.next().getFields().get(0).getLongV());
      }
    } catch (Exception e) {
      LOGGER.warn("Error occurred when trying to restore stateful trigger", e);
    }
    LOGGER.info("###### restore ##########");
  }

  @Override
  public void onDrop() throws Exception {
    LOGGER.info("********** onDrop() ***********");
    if (session != null) {
      session.close();
      updateFuture.cancel(true);
    }
  }

  @Override
  public FailureStrategy getFailureStrategy() {
    return FailureStrategy.OPTIMISTIC;
  }

  private void ensureSession() throws IoTDBConnectionException {
    if (session == null) {
      session = new Session.Builder().host(ip).port(port).build();
      session.open(false);
      updateFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              triggerInformationUpdateExecutor,
              this::updateTask,
              UPDATE_INTERVAL,
              UPDATE_INTERVAL,
              TimeUnit.MILLISECONDS);
      LOGGER.info("Stateful-Trigger-Statistics-Updater is successfully started.");
    }
  }

  private void updateTask() {
    try {
      this.session.insertRecord(
          TARGET_DEVICE,
          new Date().getTime(),
          Collections.singletonList(TARGET_SERIES),
          Collections.singletonList(TSDataType.INT64),
          cnt.get());
    } catch (Exception e) {
      LOGGER.warn("Error occurred in updateTask", e);
    }
  }
}
