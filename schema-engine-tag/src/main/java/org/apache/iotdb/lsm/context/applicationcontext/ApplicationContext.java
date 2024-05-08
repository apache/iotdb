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
package org.apache.iotdb.lsm.context.applicationcontext;

import java.util.List;

/**
 * Indicates the information of a user program, the user can customize the configuration startup
 * information, and the lsm framework can use this information to generate the lsm engine
 */
public class ApplicationContext {

  // Save the insertion level processor of each layer in hierarchical order
  List<String> insertionLevelProcessClass;

  // Save the deletion level processor of each layer in hierarchical order
  List<String> deletionLevelProcessClass;

  // Save the query level processor of each layer in hierarchical order
  List<String> queryLevelProcessClass;

  public List<String> getInsertionLevelProcessClass() {
    return insertionLevelProcessClass;
  }

  public void setInsertionLevelProcessClass(List<String> insertionLevelProcessClass) {
    this.insertionLevelProcessClass = insertionLevelProcessClass;
  }

  public List<String> getDeletionLevelProcessClass() {
    return deletionLevelProcessClass;
  }

  public void setDeletionLevelProcessClass(List<String> deletionLevelProcessClass) {
    this.deletionLevelProcessClass = deletionLevelProcessClass;
  }

  public List<String> getQueryLevelProcessClass() {
    return queryLevelProcessClass;
  }

  public void setQueryLevelProcessClass(List<String> queryLevelProcessClass) {
    this.queryLevelProcessClass = queryLevelProcessClass;
  }
}
