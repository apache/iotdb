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
package org.apache.iotdb.backup.core.service;

import org.apache.iotdb.backup.core.exception.FileTransFormationException;
import org.apache.iotdb.backup.core.model.ValidationType;
import org.apache.iotdb.backup.core.parse.CsvFileTransParser;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;

import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** @Author: LL @Description: @Date: create in 2022/8/3 9:53 */
public class CsvFileValidationForPipelineService extends CsvFileValidationService {

  @Override
  public void dataValidateWithServer(
      String path, Session session, String charset, ValidationType type) throws Exception {
    File fi = validateFilePath(path);
    long size = fi.length();
    if (size >= 200 * 1024 * 1024) {
      throw new FileTransFormationException("the max supported file size is 200MB");
    }
    if (!fi.getName().endsWith(".csv")) {
      throw new FileTransFormationException("given file is not a csv file");
    }

    CsvFileTransParser parser = new CsvFileTransParser(fi, charset);
    List<CSVRecord> allList = new ArrayList<>();
    List<CSVRecord> batchList = new ArrayList();
    try {
      while ((batchList = parser.nextBatchRecords(10000)).size() != 0) {
        allList.addAll(batchList);
      }
    } catch (Exception e) {
    } finally {
      parser.close();
    }
    if (allList.size() == 0) {
      return;
    }

    List<String> parserHeader = parser.csvParser.getHeaderNames();
    List<String> timeseries = new ArrayList<>();
    timeseries.addAll(parserHeader);
    timeseries.remove("Time");
    String deviceName = timeseries.get(0).substring(0, timeseries.get(0).lastIndexOf("."));
    Map<String, String> headMap = this.getFiledTypeMap(timeseries);
    doDataValidation(allList, timeseries, headMap, session, type);
  }

  public Map<String, String> getFiledTypeMap(List<String> timeseries)
      throws StatementExecutionException, IoTDBConnectionException {
    Map<String, String> typeMap = new HashMap<>();
    for (String t : timeseries) {
      typeMap.put(t, t);
    }
    return typeMap;
  }
}
