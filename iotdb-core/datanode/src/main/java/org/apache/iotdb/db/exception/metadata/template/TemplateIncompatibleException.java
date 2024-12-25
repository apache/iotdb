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
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.rpc.TSStatusCode;

public class TemplateIncompatibleException extends MetadataException {

  public TemplateIncompatibleException(
      String path, String templateName, PartialPath templateSetPath) {
    super(
        String.format(
            "Cannot create timeseries [%s] since device template [%s] already set on path [%s].",
            path, templateName, templateSetPath),
        TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode(),
        true);
  }

  public TemplateIncompatibleException(String templateName, PartialPath templateSetPath) {
    super(
        String.format(
            "Cannot set device template [%s] to path [%s] "
                + "since there's timeseries under path [%s].",
            templateName, templateSetPath, templateSetPath),
        TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode(),
        true);
  }

  public TemplateIncompatibleException(String reason) {
    super(reason, TSStatusCode.TEMPLATE_INCOMPATIBLE.getStatusCode(), true);
  }
}
