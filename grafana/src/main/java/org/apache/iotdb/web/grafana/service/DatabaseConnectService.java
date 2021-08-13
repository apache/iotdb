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
package org.apache.iotdb.web.grafana.service;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.web.grafana.bean.TimeValues;

import java.time.ZonedDateTime;
import java.util.List;

public interface DatabaseConnectService {

  int testConnection();

  List<TimeValues> querySeries(String s, Pair<ZonedDateTime, ZonedDateTime> timeRange);

  List<String> getMetaData();
}
