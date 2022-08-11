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

package org.apache.iotdb.tool.core.model;

import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.util.ArrayList;
import java.util.List;

public class DsTypeEncodeModel {

  private String typeName;

  private List<String> encodeNameList = new ArrayList<>();

  private List<Encoder> encoders = new ArrayList<>();

  private List<PublicBAOS> publicBAOS = new ArrayList<>();

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public List<String> getEncodeNameList() {
    return encodeNameList;
  }

  public void setEncodeNameList(List<String> encodeNameList) {
    this.encodeNameList = encodeNameList;
  }

  public List<Encoder> getEncoders() {
    return encoders;
  }

  public void setEncoders(List<Encoder> encoders) {
    this.encoders = encoders;
  }

  public List<PublicBAOS> getPublicBAOS() {
    return publicBAOS;
  }

  public void setPublicBAOS(List<PublicBAOS> publicBAOS) {
    this.publicBAOS = publicBAOS;
  }
}
