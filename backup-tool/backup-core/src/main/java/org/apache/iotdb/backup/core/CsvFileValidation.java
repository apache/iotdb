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
package org.apache.iotdb.backup.core;

import org.apache.iotdb.backup.core.model.ValidationType;
import org.apache.iotdb.backup.core.service.CsvFileValidationService;
import org.apache.iotdb.backup.core.service.CsvFileValidationService0_13_1;
import org.apache.iotdb.session.Session;

public class CsvFileValidation {

  /**
   * this is for the csv file exported by v0.13.0 compare the content in the path file with the data
   * in iotdb this method need session,and it do not close the session,you need to manage the
   * session by yourself
   *
   * @param path
   * @param session
   * @throws Exception
   */
  @Deprecated
  public static void dataValidation(
      String path, Session session, String charset, ValidationType type) throws Exception {
    CsvFileValidationService validationService = new CsvFileValidationService();
    validationService.dataValidateWithServer(path, session, charset, type);
  }

  /**
   * this is for the csv file exported by v0.13.1
   *
   * @param path
   * @param session
   * @param charset
   * @param type
   * @throws Exception
   */
  public static void dataValidation0_13_1(
      String path, Session session, String charset, ValidationType type) throws Exception {
    CsvFileValidationService0_13_1 service0_13_1 = new CsvFileValidationService0_13_1();
    service0_13_1.dataValidateWithServer(path, session, charset, type);
  }
}
