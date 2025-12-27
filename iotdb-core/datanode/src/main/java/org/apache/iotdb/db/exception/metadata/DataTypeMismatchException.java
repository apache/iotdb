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

package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.commons.exception.MetadataException;

import org.apache.tsfile.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTypeMismatchException extends MetadataException {

  // NOTICE: DO NOT CHANGE THIS STRING, IT IS USED IN THE ERROR HANDLING OF PIPE
  public static final String REGISTERED_TYPE_STRING = "registered type";
  private static final Logger log = LoggerFactory.getLogger(DataTypeMismatchException.class);

  public DataTypeMismatchException(
      String deviceName,
      String measurementName,
      TSDataType insertType,
      TSDataType typeInSchema,
      long time,
      Object value) {
    super(
        String.format(
            "data type of %s.%s is not consistent, "
                + "%s %s, inserting type %s, timestamp %s, value %s",
            deviceName,
            measurementName,
            REGISTERED_TYPE_STRING,
            typeInSchema,
            insertType,
            time,
            value == null ? "null" : processValue(value.toString())));

    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();

    // 通常，我们需要忽略前两层堆栈信息，因为它们分别对应于printCallerInfo()方法和当前正在执行的方法（例如methodB()）
    if (stackTraceElements.length > 2) {
      // 获取调用者的信息
      StackTraceElement caller =
          stackTraceElements[2]; // 获取调用者信息，索引从0开始，所以要取第3个元素（因为第0和第1个是当前方法的堆栈）
      log.error("调用者类名: {}", caller.getClassName());
      log.error("调用者方法名: {}", caller.getMethodName());
      log.error("调用者文件名: {}", caller.getFileName());
      log.error("调用者行号: {}", caller.getLineNumber());
    } else {
      log.error("堆栈跟踪信息不足，无法确定调用者信息。");
    }
  }

  public DataTypeMismatchException(
      String deviceName, String measurementName, TSDataType insertType, long time, Object value) {
    super(
        String.format(
            "data type and value of %s.%s is not consistent, "
                + "inserting type %s, timestamp %s, value %s",
            deviceName,
            measurementName,
            insertType,
            time,
            value == null ? "null" : processValue(value.toString())));
    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();

    // 通常，我们需要忽略前两层堆栈信息，因为它们分别对应于printCallerInfo()方法和当前正在执行的方法（例如methodB()）
    if (stackTraceElements.length > 2) {
      // 获取调用者的信息
      StackTraceElement caller =
          stackTraceElements[2]; // 获取调用者信息，索引从0开始，所以要取第3个元素（因为第0和第1个是当前方法的堆栈）
      log.error("调用者类名: {}", caller.getClassName());
      log.error("调用者方法名: {}", caller.getMethodName());
      log.error("调用者文件名: {}", caller.getFileName());
      log.error("调用者行号: {}", caller.getLineNumber());
    } else {
      log.error("堆栈跟踪信息不足，无法确定调用者信息。");
    }
  }

  private static String processValue(String value) {
    return value.length() < 100 ? value : value.substring(0, 100);
  }
}
