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
package org.apache.iotdb.tsfile.exception.filter;

import org.apache.iotdb.tsfile.exception.TsFileRuntimeException;

public class StatisticsClassException extends TsFileRuntimeException {

  private static final long serialVersionUID = -5445795844780183770L;

  public StatisticsClassException(Class<?> className1, Class<?> className2) {
    super("Statistics classes mismatched: " + className1 + " vs. " + className2);
  }

  public StatisticsClassException(String message) {
    super(message);
  }
}
