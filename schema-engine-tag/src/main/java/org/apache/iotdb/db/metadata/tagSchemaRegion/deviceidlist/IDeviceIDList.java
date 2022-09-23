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
package org.apache.iotdb.db.metadata.tagSchemaRegion.deviceidlist;

import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.metadata.idtable.entry.IDeviceID;

import java.io.IOException;
import java.util.List;

/** manage device id -> int32 id */
public interface IDeviceIDList {

  /**
   * insert a device id
   *
   * @param deviceID device id
   */
  void add(IDeviceID deviceID);

  /**
   * get device id using int32 id
   *
   * @param index int32 id
   * @return device id
   */
  IDeviceID get(int index);

  /**
   * returns the number of managed device ids
   *
   * @return the number of managed device ids
   */
  int size();

  /**
   * get all managed device ids
   *
   * @return all managed device ids
   */
  List<IDeviceID> getAllDeviceIDS();

  @TestOnly
  void clear() throws IOException;
}
