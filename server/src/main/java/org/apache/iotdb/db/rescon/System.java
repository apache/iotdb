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

import org.apache.iotdb.db.engine.storagegroup.TsFileProcessor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class System {

  long total_tsp_info_mem_cost;
  long array_pool_mem_cost;
  boolean reject = false;

  /**
   * 通知system申请新数组。(调用者应在实际申请前调用该方法) 当内存够用时，返回同意，否则不同意。 若不同意，则设置自身reject为true，触发flush。
   *
   * @param type type
   * @param size size
   * @return 如果同意，则返回true；否则返回false。
   */
  public boolean applyNewOOBArray(TSDataType type, int size) {
    return false;
  }

  /**
   * 通知system自身tsp内存将发生变化。 当内存够用时，返回同意，否则不同意。 若不同意，则设置自身reject为false，触发flush。
   *
   * @param processor processor
   * @param delta 变化量，大于等于0.
   */
  public boolean reportTsFileProcessorStatus(TsFileProcessor processor, long delta) {
    return false;
  }

  /**
   * 通知system将释放OOP数组（在释放后调用）。 设置自身reject为false
   *
   * @param type type
   * @param size size
   */
  public void releaseOOBArray(TSDataType type, int size) {

  }

  /**
   * 通知system将重置processor的内存占用量 （关闭文件后调用）。 设置自身reject为false
   *
   * @param processor processor
   * @param original 原有值
   */
  public void resetTsFileProcessorStatus(TsFileProcessor processor, long original) {

  }

  /**
   * 触发刷写。 若发现队列队长大于k，则不触发，否则触发。
   */
  public void flush() {

  }
}
