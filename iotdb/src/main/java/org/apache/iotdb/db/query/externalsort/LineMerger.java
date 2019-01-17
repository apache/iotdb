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
// import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairDeserializer;
// import org.apache.iotdb.db.query.externalsort.serialize.TimeValuePairSerializer;
// import org.apache.iotdb.db.query.externalsort.serialize.impl.FixLengthTimeValuePairDeserializer;
// import org.apache.iotdb.db.query.externalsort.serialize.impl.FixLengthTimeValuePairSerializer;
// import org.apache.iotdb.db.query.reader.merge.PriorityMergeReader;
// import org.apache.iotdb.db.query.reader.merge.PrioritySeriesReader;
//
// import java.io.IOException;
// import java.util.List;
//
//
// public class LineMerger {
//
// private String tmpFilePath;
//
// public LineMerger(String tmpFilePath) {
// this.tmpFilePath = tmpFilePath;
// }
//
// public PrioritySeriesReader merge(List<PrioritySeriesReader> prioritySeriesReaders) throws IOException {
// TimeValuePairSerializer serializer = new FixLengthTimeValuePairSerializer(tmpFilePath);
// PriorityMergeReader reader = new PriorityMergeReader(prioritySeriesReaders);
// while (reader.hasNext()) {
// serializer.write(reader.next());
// }
// reader.close();
// serializer.close();
// TimeValuePairDeserializer deserializer = new FixLengthTimeValuePairDeserializer(tmpFilePath);
// return new PrioritySeriesReader(deserializer, prioritySeriesReaders.get(0).getPriority());
// }
// }
