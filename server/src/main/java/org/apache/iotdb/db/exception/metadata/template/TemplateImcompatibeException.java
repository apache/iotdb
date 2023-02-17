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
 *
 */

package org.apache.iotdb.db.exception.metadata.template;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.rpc.TSStatusCode;

public class TemplateImcompatibeException extends MetadataException {

  public TemplateImcompatibeException(String path, String templateName) {
    super(
        String.format("Path [%s] already exists in [%s]", path, templateName),
        TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode());
    this.isUserException = true;
  }

  public TemplateImcompatibeException(String path, String templateName, String overlapNodeName) {
    super(
        String.format("Path [%s] overlaps with [%s] on [%s]", path, templateName, overlapNodeName),
        TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode());
    this.isUserException = true;
  }
}
