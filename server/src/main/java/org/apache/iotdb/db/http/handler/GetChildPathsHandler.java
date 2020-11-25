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
package org.apache.iotdb.db.http.handler;

import com.google.gson.JsonArray;
import java.util.Set;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.service.IoTDB;

public class GetChildPathsHandler extends Handler{
  public JsonArray handle(String path) throws MetadataException {
    PartialPath partialPath = new PartialPath(path);
    Set<String> childNodeNames = IoTDB.metaManager.getChildNodeNameInNextLevel(partialPath);
    JsonArray childNodeNamesJson = new JsonArray();
    for(String childNodeName : childNodeNames) {
      childNodeNamesJson.add(childNodeName);
    }
    return childNodeNamesJson;
  }
}
