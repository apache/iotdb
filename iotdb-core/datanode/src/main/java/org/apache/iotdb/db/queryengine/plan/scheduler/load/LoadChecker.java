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

package org.apache.iotdb.db.queryengine.plan.scheduler.load;

import org.apache.iotdb.db.exception.LoadFileException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadChecker {
  private final Map<String, List<LoadFileException>> fileToExceptionsMap = new HashMap<>();

  public void addException(String tsfilePath, LoadFileException e) {
    fileToExceptionsMap.computeIfAbsent(tsfilePath, k -> new ArrayList<>()).add(e);
  }

  public Map<String, List<LoadFileException>> getExceptionsMap() {
    return fileToExceptionsMap;
  }

  public void clean() {
    fileToExceptionsMap.clear();
  }

  ///////////////// SINGLETON /////////////////
  private LoadChecker() {
    // do nothing
  }

  private static class LoadCheckerHolder {
    private static final LoadChecker INSTANCE = new LoadChecker();
  }

  public static LoadChecker getInstance() {
    return LoadCheckerHolder.INSTANCE;
  }
}
