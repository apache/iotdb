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

package org.apache.iotdb.flink.tsfile;

import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

import java.io.Serializable;

/**
 * RowRecordParser parses the RowRecord objects read from TsFile into the user desired format. If
 * the accurate type information of parse result can not be extracted from the result type class
 * automatically (e.g. Row, Tuple, etc.), the {@link ResultTypeQueryable} interface needs to be
 * implemented to provide the type information explicitly.
 *
 * @param <T> The type of the parse result.
 */
public interface RowRecordParser<T> extends Serializable {

  /**
   * Parse the row record into type T. The param `reuse` is recommended to use for reducing the
   * creation of new objects.
   *
   * @param rowRecord The input row record.
   * @param reuse The object could be reused.
   * @return The parsed result.
   */
  T parse(RowRecord rowRecord, T reuse);
}
