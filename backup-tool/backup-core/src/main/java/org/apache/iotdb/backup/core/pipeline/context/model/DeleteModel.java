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
package org.apache.iotdb.backup.core.pipeline.context.model;

import org.apache.iotdb.session.Session;

import java.util.List;

/** @Author: LL @Description: @Date: create in 2022/7/26 13:51 */
public class DeleteModel extends PipelineModel {

  private Session session;

  private String iotdbPath;

  private String whereClause;

  private List<String> measurementList;

  public Session getSession() {
    return session;
  }

  public void setSession(Session session) {
    this.session = session;
  }

  public String getIotdbPath() {
    return iotdbPath;
  }

  public void setIotdbPath(String iotdbPath) {
    this.iotdbPath = iotdbPath;
  }

  public String getWhereClause() {
    return whereClause;
  }

  public void setWhereClause(String whereClause) {
    this.whereClause = whereClause;
  }

  public List<String> getMeasurementList() {
    return measurementList;
  }

  public void setMeasurementList(List<String> measurementList) {
    this.measurementList = measurementList;
  }
}
