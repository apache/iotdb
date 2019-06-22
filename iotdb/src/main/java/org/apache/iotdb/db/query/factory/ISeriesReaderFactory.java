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
package org.apache.iotdb.db.query.factory;

import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

/**
 * This class defines the interface of construct readers for different data source. Note that, job
 * id equals -1 meant that this method is used for IoTDB merge process, it's no need to maintain the
 * opened file stream.
 */
public interface ISeriesReaderFactory {


  /**
   * This method is used to read all unsequence data for IoTDB request, such as query, aggregation
   * and groupby request.
   */
  IPointReader createUnSeqReader(Path seriesPath, List<TsFileResourceV2> unSeqResources, QueryContext context,
                                 Filter filter) throws IOException;


  /**
   * construct ByTimestampReader, including sequential data and unsequential data.
   *
   * @param paths   selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  List<EngineReaderByTimeStamp> createByTimestampReadersOfSelectedPaths(List<Path> paths,
                                                                        QueryContext context) throws IOException, FileNodeManagerException;

  /**
   * construct IPointReader, include sequential data and unsequential data.
   *
   * @param path    selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  IPointReader createAllDataReader(Path path, Filter timeFilter,
                                   QueryContext context) throws FileNodeManagerException;
}
