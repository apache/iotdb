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

public class EncodeCompressAnalysedModel {
  private String typeName;

  private String encodeName;

  private String compressName;

  private long originSize;

  private long encodedSize;

  private long uncompressSize;

  private long compressedSize;

  private long compressedCost;

  private double score;

  public String getTypeName() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName = typeName;
  }

  public String getEncodeName() {
    return encodeName;
  }

  public void setEncodeName(String encodeName) {
    this.encodeName = encodeName;
  }

  public String getCompressName() {
    return compressName;
  }

  public void setCompressName(String compressName) {
    this.compressName = compressName;
  }

  public long getOriginSize() {
    return originSize;
  }

  public void setOriginSize(long originSize) {
    this.originSize = originSize;
  }

  public long getEncodedSize() {
    return encodedSize;
  }

  public void setEncodedSize(long encodedSize) {
    this.encodedSize = encodedSize;
  }

  public long getUncompressSize() {
    return uncompressSize;
  }

  public void setUncompressSize(long uncompressSize) {
    this.uncompressSize = uncompressSize;
  }

  public long getCompressedSize() {
    return compressedSize;
  }

  public void setCompressedSize(long compressedSize) {
    this.compressedSize = compressedSize;
  }

  public long getCompressedCost() {
    return compressedCost;
  }

  public void setCompressedCost(long compressedCost) {
    this.compressedCost = compressedCost;
  }

  public double getScore() {
    return score;
  }

  public void setScore(double score) {
    this.score = score;
  }

  public String toString() {
    return "typeName : "
        + typeName
        + " compressName : "
        + compressName
        + " encodeName : "
        + encodeName
        + " score : "
        + score
        + " compressed cost : "
        + compressedCost
        + " compressedSize : "
        + compressedSize
        + " uncompressedSize : "
        + uncompressSize
        + " encodedSize : "
        + encodedSize
        + " originSize : "
        + originSize;
  }
}
