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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex;

import org.apache.iotdb.commons.utils.TestOnly;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** tag inverted index interface */
public interface ITagInvertedIndex {

  /**
   * insert tags and device id
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  void addTags(Map<String, String> tags, int id);

  /**
   * delete tags and id using delete request context
   *
   * @param tags tags like: <tagKey,tagValue>
   * @param id INT32 device id
   */
  void removeTags(Map<String, String> tags, int id);

  /**
   * get all matching device ids
   *
   * @param tags tags like: <tagKey,tagValue>
   * @return device ids
   */
  List<Integer> getMatchedIDs(Map<String, String> tags);

  /**
   * Close all open resources
   *
   * @throws IOException
   */
  @TestOnly
  void clear() throws IOException;
}
