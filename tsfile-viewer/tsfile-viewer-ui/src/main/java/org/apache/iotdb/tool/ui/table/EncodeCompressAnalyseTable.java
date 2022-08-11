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

package org.apache.iotdb.tool.ui.table;

import javafx.beans.property.SimpleDoubleProperty;
import javafx.beans.property.SimpleLongProperty;
import javafx.beans.property.SimpleStringProperty;

/**
 * EncodeCompressAnalyseTable
 *
 * @author shenguanchu
 */
public class EncodeCompressAnalyseTable {
  private final SimpleStringProperty typeName;

  private final SimpleStringProperty encodeName;

  private final SimpleStringProperty compressName;

  private final SimpleLongProperty originSize;

  private final SimpleLongProperty encodedSize;

  private final SimpleLongProperty uncompressSize;

  private final SimpleLongProperty compressedSize;

  private final SimpleDoubleProperty compressedRatio;

  private final SimpleDoubleProperty compressedCost;

  private final SimpleDoubleProperty score;

  public EncodeCompressAnalyseTable(
      String typeName,
      String encodeName,
      String compressName,
      Long originSize,
      Long encodedSize,
      Long uncompressSize,
      Long compressedSize,
      Double compressedCost,
      Double compressedRatio,
      Double score) {
    this.typeName = new SimpleStringProperty(typeName);
    this.encodeName = new SimpleStringProperty(encodeName);
    this.compressName = new SimpleStringProperty(compressName);
    this.originSize = new SimpleLongProperty(originSize);
    this.encodedSize = new SimpleLongProperty(encodedSize);
    this.uncompressSize = new SimpleLongProperty(uncompressSize);
    this.compressedSize = new SimpleLongProperty(compressedSize);
    this.compressedCost = new SimpleDoubleProperty(compressedCost);
    this.compressedRatio = new SimpleDoubleProperty(compressedRatio);
    this.score = new SimpleDoubleProperty(score);
  }

  public String getTypeName() {
    return typeName.get();
  }

  public SimpleStringProperty typeNameProperty() {
    return typeName;
  }

  public void setTypeName(String typeName) {
    this.typeName.set(typeName);
  }

  public String getEncodeName() {
    return encodeName.get();
  }

  public SimpleStringProperty encodeNameProperty() {
    return encodeName;
  }

  public void setEncodeName(String encodeName) {
    this.encodeName.set(encodeName);
  }

  public String getCompressName() {
    return compressName.get();
  }

  public SimpleStringProperty compressNameProperty() {
    return compressName;
  }

  public void setCompressName(String compressName) {
    this.compressName.set(compressName);
  }

  public long getOriginSize() {
    return originSize.get();
  }

  public SimpleLongProperty originSizeProperty() {
    return originSize;
  }

  public void setOriginSize(long originSize) {
    this.originSize.set(originSize);
  }

  public long getEncodedSize() {
    return encodedSize.get();
  }

  public SimpleLongProperty encodedSizeProperty() {
    return encodedSize;
  }

  public void setEncodedSize(long encodedSize) {
    this.encodedSize.set(encodedSize);
  }

  public long getUncompressSize() {
    return uncompressSize.get();
  }

  public SimpleLongProperty uncompressSizeProperty() {
    return uncompressSize;
  }

  public void setUncompressSize(long uncompressSize) {
    this.uncompressSize.set(uncompressSize);
  }

  public long getCompressedSize() {
    return compressedSize.get();
  }

  public SimpleLongProperty compressedSizeProperty() {
    return compressedSize;
  }

  public void setCompressedSize(long compressedSize) {
    this.compressedSize.set(compressedSize);
  }

  public double getCompressedCost() {
    return compressedCost.get();
  }

  public SimpleDoubleProperty compressedCostProperty() {
    return compressedCost;
  }

  public void setCompressedCost(double compressedCost) {
    this.compressedCost.set(compressedCost);
  }

  public double getScore() {
    return score.get();
  }

  public SimpleDoubleProperty scoreProperty() {
    return score;
  }

  public void setScore(double score) {
    this.score.set(score);
  }

  public double getCompressedRatio() {
    return compressedRatio.get();
  }

  public SimpleDoubleProperty compressedRatioProperty() {
    return compressedRatio;
  }

  public void setCompressedRatio(double compressedRatio) {
    this.compressedRatio.set(compressedRatio);
  }
}
