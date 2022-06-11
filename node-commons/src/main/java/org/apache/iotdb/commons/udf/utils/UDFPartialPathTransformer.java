/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.commons.udf.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.udf.api.commons.UDFPartialPath;

import java.util.List;
import java.util.stream.Collectors;

public class UDFPartialPathTransformer {

  private UDFPartialPathTransformer() {}

  public static UDFPartialPath transformToUDFPartialPath(PartialPath partialPath) {
    return partialPath == null ? null : new UDFPartialPath(partialPath.getNodes());
  }

  public static List<UDFPartialPath> transformToUDFPartialPathList(
      List<PartialPath> partialPathList) {
    return partialPathList == null
        ? null
        : partialPathList.stream()
            .map(UDFPartialPathTransformer::transformToUDFPartialPath)
            .collect(Collectors.toList());
  }

  public static PartialPath transformToPartialPath(UDFPartialPath udfPartialPath)
      throws IllegalPathException {
    return udfPartialPath == null ? null : new PartialPath(udfPartialPath.getFullPath());
  }
}
