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
package org.apache.iotdb.db.engine.storagegroup;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.util.List;

/** It's an interface that storage engine must provide for query engine */
public interface IDataRegionForQuery {

  /** lock the read lock for thread-safe */
  void readLock();

  void readUnlock();

  /** Get satisfied QueryDataSource from DataRegion */
  QueryDataSource query(
      List<PartialPath> pathList, String singleDeviceId, QueryContext context, Filter timeFilter)
      throws QueryProcessException;

  /** Get TTL of this DataRegion */
  long getDataTTL();

  /** Get database name of this DataRegion */
  String getDatabaseName();
}
