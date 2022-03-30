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
package org.apache.iotdb.db.newsync.receiver.load;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.service.IoTDB;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * This loader is used to PhysicalPlan. Support four types of physical plans: CREATE_TIMESERIES |
 * CREATE_ALIGNED_TIMESERIES | DELETE_TIMESERIES | SET_STORAGE_GROUP
 */
public class SchemaLoader implements ILoader {
  private static final Logger logger = LoggerFactory.getLogger(SchemaLoader.class);

  private PhysicalPlan plan;

  public SchemaLoader(PhysicalPlan plan) {
    this.plan = plan;
  }

  @Override
  public void load() throws IOException, MetadataException {
    IoTDB.schemaProcessor.operation(plan);
  }
}
