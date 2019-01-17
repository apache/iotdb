/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
// package org.apache.iotdb.db.query.externalsort;
//
// import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//
// import java.io.IOException;
// import java.util.List;
//
//
// public interface ExternalSortJobEngine {
//
// /**
// * Receive a list of TimeValuePairReaders and judge whether it should be processed
// using external sort.
// * If needed, do the merge sort for all TimeValuePairReaders using specific strategy.
// * @param timeValuePairReaderList A list include a set of TimeValuePairReaders
// * @return
// */
// List<PrioritySeriesReader> executeWithGlobalTimeFilter(List<PrioritySeriesReader>
// timeValuePairReaderList) throws
// IOException;
//
// /**
// * Create an external sort job which contains many parts.
// * @param timeValuePairReaderList
// * @return
// */
// ExternalSortJob createJob(List<PrioritySeriesReader> timeValuePairReaderList);
//
// }
