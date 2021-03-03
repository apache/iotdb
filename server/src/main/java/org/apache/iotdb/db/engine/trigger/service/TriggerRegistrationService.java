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

package org.apache.iotdb.db.engine.trigger.service;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.DropTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.ShowTriggersPlan;
import org.apache.iotdb.db.qp.physical.sys.StartTriggerPlan;
import org.apache.iotdb.db.qp.physical.sys.StopTriggerPlan;
import org.apache.iotdb.db.query.dataset.ListDataSet;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;

import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_ATTRIBUTES;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_CLASS;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_EVENT;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_NAME;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_PATH;
import static org.apache.iotdb.db.conf.IoTDBConstant.COLUMN_TRIGGER_STATUS;

public class TriggerRegistrationService implements IService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TriggerRegistrationService.class);

  private static final String TLOG_FILE_DIR =
      IoTDBDescriptor.getInstance().getConfig().getSystemDir()
          + File.separator
          + "trigger"
          + File.separator;
  private static final String LOG_FILE_NAME = TLOG_FILE_DIR + "tlog.bin";
  private static final String TEMPORARY_LOG_FILE_NAME = LOG_FILE_NAME + ".tmp";

  public void register(CreateTriggerPlan plan) {}

  public void deregister(DropTriggerPlan plan) {}

  public void activate(StartTriggerPlan plan) {}

  public void inactivate(StopTriggerPlan plan) {}

  public QueryDataSet show(ShowTriggersPlan plan) {
    return new ListDataSet(
        Arrays.asList(
            new PartialPath(COLUMN_TRIGGER_NAME, false),
            new PartialPath(COLUMN_TRIGGER_STATUS, false),
            new PartialPath(COLUMN_TRIGGER_EVENT, false),
            new PartialPath(COLUMN_TRIGGER_PATH, false),
            new PartialPath(COLUMN_TRIGGER_CLASS, false),
            new PartialPath(COLUMN_TRIGGER_ATTRIBUTES, false)),
        Arrays.asList(
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT,
            TSDataType.TEXT));
  }

  @Override
  public void start() throws StartupException {}

  @Override
  public void stop() {}

  @Override
  public ServiceType getID() {
    return ServiceType.TRIGGER_REGISTRATION_SERVICE;
  }

  public static TriggerRegistrationService getInstance() {
    return TriggerRegistrationService.TriggerRegistrationServiceHelper.INSTANCE;
  }

  private static class TriggerRegistrationServiceHelper {

    private static final TriggerRegistrationService INSTANCE = new TriggerRegistrationService();

    private TriggerRegistrationServiceHelper() {}
  }
}
