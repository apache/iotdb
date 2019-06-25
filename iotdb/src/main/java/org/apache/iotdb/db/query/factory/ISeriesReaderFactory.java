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

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.filenodeV2.TsFileResourceV2;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.merge.EngineReaderByTimeStamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

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
  IPointReader createUnseqSeriesReader(Path seriesPath, List<TsFileResourceV2> unSeqResources,
                                       QueryContext context,
                                       Filter filter) throws IOException;


  /**
   * construct ByTimestampReader, including sequence data and unsequence data.
   *
   * @param paths selected series path
   * @param context query context
   * @return the list of EngineReaderByTimeStamp
   */
  List<EngineReaderByTimeStamp> createSeriesReadersByTimestamp(List<Path> paths,
                                                               QueryContext context) throws FileNodeManagerException, IOException;

  /**
   * construct IPointReader with <br>only time filter or no filter</br>, including sequence data
   * and unsequence data. This reader won't filter the result of merged sequence data and
   * unsequence data reader.
   *
   * @param path selected series path
   * @param timeFilter time filter or null
   * @param context query context
   * @return data reader including seq and unseq data source.
   */
  IPointReader createSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
                                                    QueryContext context) throws FileNodeManagerException, IOException;

  /**
   * construct IPointReader with <br>value filter</br>, include sequence data and unsequence
   * data. This reader will filter the result of merged sequence data and unsequence data
   * reader, so if only has time filter please call createSeriesReaderWithoutValueFilter().
   *
   * @param path selected series path
   * @param filter time filter or null
   * @param context query context
   * @return data reader including seq and unseq data source.
   */
  IPointReader createSeriesReaderWithValueFilter(Path path, Filter filter, QueryContext context)
      throws FileNodeManagerException, IOException;
}
