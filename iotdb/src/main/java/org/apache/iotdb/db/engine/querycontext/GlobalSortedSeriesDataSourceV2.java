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
package org.apache.iotdb.db.engine.querycontext;

import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.tsfile.read.common.Path;

public class GlobalSortedSeriesDataSourceV2 {

  private Path seriesPath;

  // including sealed tsfile and unsealed tsfile
  private List<TsFileResourceV2> queryTsFiles;


  public GlobalSortedSeriesDataSourceV2(Path seriesPath, List<TsFileResourceV2> queryTsFiles) {
    this.seriesPath = seriesPath;
    this.queryTsFiles = queryTsFiles;
  }

  public Path getSeriesPath() {
    return seriesPath;
  }

  public void setSeriesPath(Path seriesPath) {
    this.seriesPath = seriesPath;
  }

  public List<TsFileResourceV2> getQueryTsFiles() {
    return queryTsFiles;
  }

  public void setQueryTsFiles(
      List<TsFileResourceV2> queryTsFiles) {
    this.queryTsFiles = queryTsFiles;
  }
}
