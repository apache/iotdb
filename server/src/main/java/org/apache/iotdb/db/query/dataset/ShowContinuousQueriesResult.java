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

package org.apache.iotdb.db.query.dataset;

import org.apache.iotdb.db.metadata.PartialPath;

public class ShowContinuousQueriesResult extends ShowResult {

  private String querySql;
  private String continuousQueryName;
  private PartialPath targetPath;
  private long everyInterval;
  private long forInterval;

  public ShowContinuousQueriesResult(
      String querySql,
      String continuousQueryName,
      PartialPath targetPath,
      long everyInterval,
      long forInterval) {
    this.querySql = querySql;
    this.continuousQueryName = continuousQueryName;
    this.targetPath = targetPath;
    this.everyInterval = everyInterval;
    this.forInterval = forInterval;
  }

  public String getQuerySql() {
    return querySql;
  }

  public void setQuerySql(String querySql) {
    this.querySql = querySql;
  }

  public String getContinuousQueryName() {
    return continuousQueryName;
  }

  public void setContinuousQueryName(String continuousQueryName) {
    this.continuousQueryName = continuousQueryName;
  }

  public PartialPath getTargetPath() {
    return targetPath;
  }

  public void setTargetPath(PartialPath targetPath) {
    this.targetPath = targetPath;
  }

  public long getEveryInterval() {
    return everyInterval;
  }

  public void setEveryInterval(long everyInterval) {
    this.everyInterval = everyInterval;
  }

  public long getForInterval() {
    return forInterval;
  }

  public void setForInterval(long forInterval) {
    this.forInterval = forInterval;
  }
}
