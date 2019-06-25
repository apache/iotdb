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
import org.apache.iotdb.db.query.reader.IReaderByTimeStamp;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

import java.io.IOException;
import java.util.List;

/**
 * This class defines the interface of constructing readers for different data source. Note that, job
 * id equals -1 meant that this method is used for IoTDB merge process, it's no need to maintain the
 * opened file stream.
 */
public interface ISeriesReaderFactory {


  /*
   * This method is used to read unsequence data for IoTDB request, such as query, aggregation
   * and groupby request.
   *
   * @param seriesPath the path of the time series to be read
   * @param unseqResources unsequence data in the seriesPath
   * @param context query context
   * @param filter It can a combination of time and value filter or null.
   * @return unsequence series reader
   */
  IPointReader createUnseqSeriesReader(Path seriesPath, List<TsFileResourceV2> unseqResources,
                                       QueryContext context,
                                       Filter filter) throws IOException;


  /**
   * construct a list of SeriesReaderByTimestamp, including sequence data and unsequence data.
   *
   * @param paths   the paths of the time series to be read
   * @param context query context
   * @return a list of IReaderByTimeStamp
   */
  List<IReaderByTimeStamp> createSeriesReadersByTimestamp(List<Path> paths,
                                                          QueryContext context) throws FileNodeManagerException, IOException;

  /**
   * construct IPointReader with <b>only time filter or no filter</b>, including sequence data
   * and unsequence data. This reader won't filter the result of merged sequence data and
   * unsequence data reader.
   *
   * @param path       the path of the time series to be read
   * @param timeFilter time filter or null
   * @param context    query context
   * @return data reader including seq and unseq data source.
   */
  IPointReader createSeriesReaderWithoutValueFilter(Path path, Filter timeFilter,
                                                    QueryContext context) throws FileNodeManagerException, IOException;

  /**
   * construct IPointReader with <b>value filter</b>, include sequence data and unsequence
   * data. This reader will filter the result of merged sequence data and unsequence data
   * reader, so if only time filter is used, please call createSeriesReaderWithoutValueFilter().
   *
   * @param path    selected series path
   * @param filter  filter that contains at least a value filter
   * @param context query context
   * @return data reader including sequence and unsequence data source.
   */
  IPointReader createSeriesReaderWithValueFilter(Path path, Filter filter, QueryContext context)
          throws FileNodeManagerException, IOException;
}
