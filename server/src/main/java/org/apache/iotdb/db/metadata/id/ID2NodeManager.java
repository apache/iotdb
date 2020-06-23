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

package org.apache.iotdb.db.metadata.id;

import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.metadata.mnode.MNode;

public class ID2NodeManager {
  List<MNode> id2strings = new ArrayList<>();


  /**
   * when calling this method, you must guarantee that all ids that less than id has been put.
   * @param id
   * @param node
   */
  public void put(int id, MNode node) {
    id2strings.set(id, node);
  }

  public MNode get(int id) {
    return id2strings.get(id);
  }
}
