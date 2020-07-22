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

package org.apache.iotdb.db.rescon;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class ArrayManager {

  long out_of_buffer_cost;

  /**
   * 根据需要的数据类型，返回一个array（成功） / Exception（失败）
   */
  public Object getDataListByType(TSDataType dataType) {

    return null;
  }

  /**
   * flush还数据时调用
   */
  public void release(Object dataArray) {

  }

  /**
   * 根据需要的数据类型，申请OOB array， 同时维护 out_of_buffer_cost （调用void updateOOBCost()）。
   * 返回cost，或者拒绝(-1)。调用system.applyOOBArray()
   */
  private long applyOOBArray(TSDataType dataType, int size) {
    return -1L;
  }


  /**
   * 调用system归还OOB的array
   */
  private void bringBackOOBArray() {

  }

}
