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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.model;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.MatchConfig.SHAPE_TOLERANCE;

public class MatchState {
  private double matchValue = 0.0;
  private boolean isFinish = false;

  private final List<Section> dataSectionList = new ArrayList<>();

  private Section patternSectionNow;

  private int startSectionId = -1;

  private int shapeNotMatch = 0;

  private double dataMaxHeight = 0.0;
  private double dataMinHeight = Double.MAX_VALUE;
  private double dataMaxWidth = 0.0;
  private double dataMinWidth = Double.MAX_VALUE;

  private double patternMaxHeight = 0.0;
  private double patternMinHeight = Double.MAX_VALUE;
  private double patternMaxWidth = 0.0;
  private double patternMinWidth = Double.MAX_VALUE;

  // this is the Gx and Gy in the paper
  private double globalWidthRadio = 0.0;
  private double globalHeightRadio = 0.0;

  public MatchState(Section patternSectionNow) {
    this.patternSectionNow = patternSectionNow;
    updatePatternBounds(patternSectionNow);
  }

  public List<Section> getDataSectionList() {
    return dataSectionList;
  }

  public Section getPatternSectionNow() {
    return patternSectionNow;
  }

  public void setPatternSectionNow(Section patternSectionNow) {
    this.patternSectionNow = patternSectionNow;
  }

  public boolean isFinish() {
    return isFinish;
  }

  public double getMatchValue() {
    return matchValue;
  }

  public int getStartSectionId() {
    return startSectionId;
  }

  public void nextPatternSection() {
    this.patternSectionNow = patternSectionNow.getNextSectionList().get(0);
    updatePatternBounds(patternSectionNow);
  }

  // use in constant automaton, because no need to record the path
  public boolean checkSign(Section section) {
    if (section.getSign() == patternSectionNow.getSign()) {
      if (startSectionId == -1) {
        startSectionId = section.getId();
      }
      updateDataBounds(section);
      return true;
    }

    shapeNotMatch++;
    if (shapeNotMatch <= SHAPE_TOLERANCE) {
      if (startSectionId == -1) {
        startSectionId = section.getId();
      }
      updateDataBounds(section);
      return true;
    }

    return false;
  }

  private void updateDataBounds(Section section) {
    if (section.getMaxHeight() > dataMaxHeight) {
      dataMaxHeight = section.getMaxHeight();
    }
    if (section.getMinHeight() < dataMinHeight) {
      dataMinHeight = section.getMinHeight();
    }
    if (section.getMaxWidth() > dataMaxWidth) {
      dataMaxWidth = section.getMaxWidth();
    }
    if (section.getMinWidth() < dataMinWidth) {
      dataMinWidth = section.getMinWidth();
    }
  }

  private void updatePatternBounds(Section section) {
    if (section.getMaxHeight() > patternMaxHeight) {
      patternMaxHeight = section.getMaxHeight();
    }
    if (section.getMinHeight() < patternMinHeight) {
      patternMinHeight = section.getMinHeight();
    }
    if (section.getMaxWidth() > patternMaxWidth) {
      patternMaxWidth = section.getMaxWidth();
    }
    if (section.getMinWidth() < patternMinWidth) {
      patternMinWidth = section.getMinWidth();
    }
  }

  public double getDataHeightBound() {
    return dataMaxHeight - dataMinHeight;
  }

  public double getDataWidthBound() {
    return dataMaxWidth - dataMinWidth;
  }

  public void calcGlobalRadio(double smoothValue) {
    globalHeightRadio =
        (dataMaxHeight - dataMinHeight) == 0
            ? smoothValue
            : (dataMaxHeight - dataMinHeight) / (patternMaxHeight - patternMinHeight) == 0
                ? smoothValue
                : (patternMaxHeight - patternMinHeight);
    globalWidthRadio = (dataMaxWidth - dataMinWidth) / (patternMaxWidth - patternMinWidth);
  }

  public boolean calcOneSectionMatchValue(Section section, double smoothValue, double threshold) {

    // calc the LED
    // this is the Rx and Ry in the paper
    double localWidthRadio =
        section.getWidthBound() / (patternSectionNow.getWidthBound() * globalWidthRadio);

    // smooth value is used to avoid the zero division
    double localHeightUp = Math.max(section.getHeightBound(), smoothValue);
    double localHeightDown =
        Math.max(patternSectionNow.getHeightBound() * globalHeightRadio, smoothValue);
    double localHeightRadio = localHeightUp / localHeightDown;

    double led = Math.pow(Math.log(localWidthRadio), 2) + Math.pow(Math.log(localHeightRadio), 2);

    // calc the SE
    // align the first point or the centroid, it's same because the calculation is just an avg
    // function, no matter where the align point is
    double alignHeightDiff =
        patternSectionNow.getPoints().get(0).y * globalHeightRadio * localHeightRadio
            - section.getPoints().get(0).y;

    // different from the origin code, in this case this code use linear fill technology to align
    // the x-axis of all point in patternSection to dataSection
    int dataPointNum = section.getPoints().size() - 1;
    int patternPointNum = patternSectionNow.getPoints().size() - 1;

    double numRadio = ((double) patternPointNum) / ((double) dataPointNum);

    double shapeError = 0.0;

    for (int i = 1; i <= dataPointNum; i++) {
      double pointHeight = getPointHeight(i, numRadio, patternPointNum);

      shapeError +=
          Math.abs(
              pointHeight * globalHeightRadio * localHeightRadio
                  - alignHeightDiff
                  - section.getPoints().get(i).y);
    }

    shapeError =
        shapeError
            / (((dataMaxHeight - dataMinHeight) == 0
                    ? smoothValue
                    : (dataMaxHeight - dataMinHeight))
                * (section.getPoints().size() - 1));

    // calc the match value for a section
    matchValue = matchValue + led + shapeError;

    dataSectionList.add(section);

    if (patternSectionNow.isFinal() && matchValue <= threshold) {
      isFinish = true;
      patternSectionNow = null;
    }

    if (isFinish || matchValue > threshold) {
      return true;
    } else {
      patternSectionNow = patternSectionNow.getNextSectionList().get(0);
      return false;
    }
  }

  private double getPointHeight(int i, double numRadio, int patternPointNum) {
    double patternIndex = i * numRadio;
    int leftIndex = (int) Math.floor(patternIndex);
    double leftRadio = patternIndex - leftIndex;
    int rightIndex = leftIndex >= patternPointNum ? patternPointNum : leftIndex + 1;
    double rightRadio = 1 - leftRadio;
    // the heightValue is a weighted avg about the index near num
    return patternSectionNow.getPoints().get(leftIndex).y * rightRadio
        + patternSectionNow.getPoints().get(rightIndex).y * leftRadio;
  }
}
