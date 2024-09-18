/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.service.metric;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class GcTimeAlerter implements JvmGcMonitorMetrics.GcTimeAlertHandler {
  @SuppressWarnings("java:S2885")
  private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private static final Logger logger = LoggerFactory.getLogger(GcTimeAlerter.class);

  /**
   * Alert handler func User can tailor their handle logic here
   *
   * @param gcData
   */
  @Override
  public void alert(JvmGcMonitorMetrics.GcData gcData) {
    logger.warn(
        "Error metrics taken time: "
            + sdf.format(new Date(Long.parseLong(String.valueOf(gcData.getTimestamp())))));
    logger.warn("Gc Time Percentage: " + gcData.getGcTimePercentage() + "%");
    logger.warn(
        "Accumulated GC time within current observation window: "
            + gcData.getGcTimeWithinObsWindow()
            + " ms");
    logger.warn(
        "The observation window is from: "
            + sdf.format(new Date(Long.parseLong(String.valueOf(gcData.getStartObsWindowTs()))))
            + " to: "
            + sdf.format(new Date(Long.parseLong(String.valueOf(gcData.getTimestamp())))));
    logger.warn("The observation window time is: " + gcData.getCurrentObsWindowTs() + " ms.");
  }
}
