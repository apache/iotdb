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

package org.apache.iotdb.db.metadata.cache;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.schematree.PathPatternTree;
import org.apache.iotdb.db.mpp.common.schematree.SchemaTree;
import org.apache.iotdb.db.mpp.plan.analyze.ClusterSchemaFetcher;
import org.apache.iotdb.db.mpp.plan.analyze.ISchemaFetcher;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DataNodeSchemaBlacklist {

  private final ISchemaFetcher schemaFetcher = ClusterSchemaFetcher.getInstance();

  private final DataNodeSchemaCache schemaCache = DataNodeSchemaCache.getInstance();

  private final Set<MeasurementPath> blacklist = Collections.synchronizedSet(new HashSet<>());

  private DataNodeSchemaBlacklist() {}

  public static DataNodeSchemaBlacklist getInstance() {
    return DataNodeSchemaBlacklist.SchemaBlacklistHolder.INSTANCE;
  }

  /** singleton pattern. */
  private static class SchemaBlacklistHolder {
    private static final DataNodeSchemaBlacklist INSTANCE = new DataNodeSchemaBlacklist();
  }

  public void appendToBlacklist(PathPatternTree patternTree) {
    List<MeasurementPath> pathList = schemaFetcher.fetchSchema(patternTree).getAllMeasurement();
    blacklist.addAll(pathList);
    for (MeasurementPath path : pathList) {
      // todo improve the implement of schemaCache
      schemaCache.invalidate(new PartialPath(path.getNodes()));
    }
  }

  public SchemaTree filterTimeseriesInBlacklist(SchemaTree schemaTree) {
    if (!blacklist.isEmpty()) {
      for (MeasurementPath measurementPath : schemaTree.getAllMeasurement()) {
        if (blacklist.contains(measurementPath)) {
          schemaTree.pruneSingleMeasurement(measurementPath);
        }
      }
    }
    return schemaTree;
  }
}
