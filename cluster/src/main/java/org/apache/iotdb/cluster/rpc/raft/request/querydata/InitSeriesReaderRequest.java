/**
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
package org.apache.iotdb.cluster.rpc.raft.request.querydata;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.iotdb.cluster.query.PathType;
import org.apache.iotdb.cluster.rpc.raft.request.BasicQueryRequest;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;

/**
 * Initially create corresponding series readers in remote query node
 */
public class InitSeriesReaderRequest extends BasicQueryRequest {

  private static final long serialVersionUID = 8374330837710097285L;

  /**
   * Unique task id which is assigned in coordinator node
   */
  private String taskId;

  /**
   * Key is series type, value is query plan
   */
  private Map<PathType, byte[]> allQueryPlan = new EnumMap<>(PathType.class);

  /**
   * Represent all filter of leaf node in filter tree while executing a query with value filter.
   */
  private List<byte[]> filterList = new ArrayList<>();


  private InitSeriesReaderRequest(String groupID, String taskId) {
    super(groupID);
    this.taskId = taskId;
  }

  public static InitSeriesReaderRequest createInitialQueryRequest(String groupId, String taskId,
      int readConsistencyLevel,
      Map<PathType, QueryPlan> allQueryPlan, List<Filter> filterList) throws IOException {
    InitSeriesReaderRequest request = new InitSeriesReaderRequest(groupId, taskId);
    request.setReadConsistencyLevel(readConsistencyLevel);
    for (Entry<PathType, QueryPlan> entry : allQueryPlan.entrySet()) {
      request.allQueryPlan.put(entry.getKey(), toByteArray(entry.getValue()));
    }
    for (Filter filter : filterList) {
      request.filterList.add(toByteArray(filter));
    }
    return request;
  }

  public String getTaskId() {
    return taskId;
  }

  public void setTaskId(String taskId) {
    this.taskId = taskId;
  }

  public Map<PathType, QueryPlan> getAllQueryPlan() throws IOException, ClassNotFoundException {
    Map<PathType, QueryPlan> queryPlanMap = new EnumMap<>(PathType.class);
    for (Entry<PathType, byte[]> entry : allQueryPlan.entrySet()) {
      queryPlanMap.put(entry.getKey(), (QueryPlan) toObject(entry.getValue()));
    }
    return queryPlanMap;
  }

  public List<Filter> getFilterList() throws IOException, ClassNotFoundException {
    List<Filter> filters = new ArrayList<>();
    for (byte[] filterBytes : filterList) {
      filters.add((Filter) toObject(filterBytes));
    }
    return filters;
  }

  /**
   * Convert an object to byte array
   *
   * @param obj Object, which need to implement Serializable
   * @return byte array of object
   */
  private static byte[] toByteArray(Object obj) throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream oos = new ObjectOutputStream(bos);
    oos.writeObject(obj);
    oos.flush();
    byte[] bytes = bos.toByteArray();
    oos.close();
    bos.close();
    return bytes;
  }

  /**
   * Convert byte array back to Object
   *
   * @param bytes byte array of object
   * @return object
   */
  private static Object toObject(byte[] bytes) throws IOException, ClassNotFoundException {
    ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
    ObjectInputStream ois = new ObjectInputStream(bis);
    Object obj = ois.readObject();
    ois.close();
    bis.close();
    return obj;
  }
}
