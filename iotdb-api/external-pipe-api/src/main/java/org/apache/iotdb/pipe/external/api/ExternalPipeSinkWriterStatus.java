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

package org.apache.iotdb.pipe.external.api;

import java.util.Map;

/** Represents the status of an external pipe sink writer. */
public class ExternalPipeSinkWriterStatus {

  private Long startTime;
  private Long numOfBytesTransmitted;
  private Long numOfRecordsTransmitted;
  private Map<String, String> extendedFields;

  public void setStartTime(Long startTime) {
    this.startTime = startTime;
  }

  public Long getStartTime() {
    return startTime;
  }

  public void setNumOfBytesTransmitted(Long numOfBytesTransmitted) {
    this.numOfBytesTransmitted = numOfBytesTransmitted;
  }

  public Long getNumOfBytesTransmitted() {
    return numOfBytesTransmitted;
  }

  public void setNumOfRecordsTransmitted(Long numOfRecordsTransmitted) {
    this.numOfRecordsTransmitted = numOfRecordsTransmitted;
  }

  public Long getNumOfRecordsTransmitted() {
    return numOfRecordsTransmitted;
  }

  public void setExtendedFields(Map<String, String> extendedFields) {
    this.extendedFields = extendedFields;
  }

  public Map<String, String> getExtendedFields() {
    return extendedFields;
  }

  @Override
  public String toString() {
    return "ExternalPipeSinkWriterStatus{"
        + "startTime="
        + startTime
        + ", numOfBytesTransmitted="
        + numOfBytesTransmitted
        + ", numOfRecordsTransmitted="
        + numOfRecordsTransmitted
        + ", extendedFields="
        + extendedFields
        + '}';
  }
}
