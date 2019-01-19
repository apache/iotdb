/**
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
package org.apache.iotdb.tsfile.qp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.iotdb.tsfile.common.utils.ITsRandomAccessFileReader;
import org.apache.iotdb.tsfile.read.query.QueryConfig;
import org.apache.iotdb.tsfile.read.query.QueryDataSet;
import org.apache.iotdb.tsfile.read.query.QueryEngine;

/**
 * This class used to execute Queries on TSFile
 */
public class Executor {

  public static List<QueryDataSet> query(ITsRandomAccessFileReader in,
      List<QueryConfig> queryConfigs, HashMap<String, Long> parameters) {
    QueryEngine queryEngine;
    List<QueryDataSet> dataSets = new ArrayList<>();
    try {
      queryEngine = new QueryEngine(in);
      for (QueryConfig queryConfig : queryConfigs) {
        QueryDataSet queryDataSet = queryEngine.query(queryConfig, parameters);
        dataSets.add(queryDataSet);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    return dataSets;
  }
}
