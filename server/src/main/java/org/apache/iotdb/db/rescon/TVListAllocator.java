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

import java.util.ArrayDeque;
import java.util.EnumMap;
import java.util.Map;
import java.util.Queue;
import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.exception.StartupException;
import org.apache.iotdb.db.nvm.datastructure.AbstractTVList;
import org.apache.iotdb.db.nvm.datastructure.NVMIntTVList;
import org.apache.iotdb.db.nvm.datastructure.NVMLongTVList;
import org.apache.iotdb.db.nvm.datastructure.NVMTVList;
import org.apache.iotdb.db.service.IService;
import org.apache.iotdb.db.service.JMXService;
import org.apache.iotdb.db.service.ServiceType;
import org.apache.iotdb.db.utils.datastructure.BinaryTVList;
import org.apache.iotdb.db.utils.datastructure.BooleanTVList;
import org.apache.iotdb.db.utils.datastructure.DoubleTVList;
import org.apache.iotdb.db.utils.datastructure.FloatTVList;
import org.apache.iotdb.db.utils.datastructure.IntTVList;
import org.apache.iotdb.db.utils.datastructure.LongTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class TVListAllocator implements TVListAllocatorMBean, IService {

  protected Map<TSDataType, Queue<TVList>> tvListCache = new EnumMap<>(TSDataType.class);
  protected Map<TSDataType, Queue<NVMTVList>> nvmTVListCache = new EnumMap<>(TSDataType.class);
  protected String mbeanName = String
      .format("%s:%s=%s", IoTDBConstant.IOTDB_PACKAGE, IoTDBConstant.JMX_TYPE,
          getID().getJmxName());

  protected static final TVListAllocator INSTANCE = new TVListAllocator();

  public static TVListAllocator getInstance() {
    return INSTANCE;
  }

  public synchronized AbstractTVList allocate(TSDataType dataType, boolean nvm) {
    AbstractTVList list = null;
    if (nvm) {
      Queue<NVMTVList> tvLists = nvmTVListCache.computeIfAbsent(dataType,
          k -> new ArrayDeque<>());
      list = tvLists.poll();
      return list != null ? list : NVMTVList.newList(dataType);
    } else {
      Queue<TVList> tvLists = tvListCache.computeIfAbsent(dataType,
          k -> new ArrayDeque<>());
      list = tvLists.poll();
      return list != null ? list : TVList.newList(dataType);
    }
  }

  public synchronized void release(TSDataType dataType, AbstractTVList list, boolean nvm) {
    list.clear();
    if (nvm) {
      nvmTVListCache.get(dataType).add((NVMTVList) list);
    } else {
      tvListCache.get(dataType).add((TVList) list);
    }
  }

  public synchronized void release(AbstractTVList list) {
    list.clear();
    if (list instanceof NVMIntTVList) {
      nvmTVListCache.get(TSDataType.INT32).add((NVMTVList) list);
    } else if (list instanceof NVMLongTVList) {
      nvmTVListCache.get(TSDataType.INT64).add((NVMTVList) list);
    } else {
      TVList tvList = (TVList) list;

      if (list instanceof BinaryTVList) {
        tvListCache.get(TSDataType.TEXT).add(tvList);
      } else if (list instanceof BooleanTVList) {
        tvListCache.get(TSDataType.BOOLEAN).add(tvList);
      } else if (list instanceof DoubleTVList) {
        tvListCache.get(TSDataType.DOUBLE).add(tvList);
      } else if (list instanceof FloatTVList) {
        tvListCache.get(TSDataType.FLOAT).add(tvList);
      } else if (list instanceof IntTVList) {
        tvListCache.get(TSDataType.INT32).add(tvList);
      } else if (list instanceof LongTVList) {
        tvListCache.get(TSDataType.INT64).add(tvList);
      }
    }
  }

  @Override
  public int getNumberOfTVLists(boolean nvm) {
    int number = 0;
    if (nvm) {
      for (Queue<NVMTVList> queue : nvmTVListCache.values()) {
        number += queue.size();
      }
    } else {
      for (Queue<TVList> queue : tvListCache.values()) {
        number += queue.size();
      }
    }
    return number;
  }

  @Override
  public void start() throws StartupException {
    try {
      JMXService.registerMBean(INSTANCE, mbeanName);
    } catch (Exception e) {
      throw new StartupException(this.getID().getName(), e.getMessage());
    }
  }

  @Override
  public void stop() {
    JMXService.deregisterMBean(mbeanName);
    tvListCache.clear();
    nvmTVListCache.clear();
  }

  @Override
  public ServiceType getID() {
    return ServiceType.TVLIST_ALLOCATOR_SERVICE;
  }
}
