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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.MatchConfig.CALC_SE_USING_MORE_MEMORY;
import static org.apache.iotdb.db.queryengine.plan.relational.function.tvf.match.MatchConfig.SHAPE_TOLERANCE;

public class RegexMatchState {

  private final Section patternSectionNow;

  private final List<Section> dataSectionList = new ArrayList<>();

  // a stack to store the pattern path
  private final Deque<Section> sectionStack = new ArrayDeque<>();

  private final Deque<PathState> matchStateStack = new ArrayDeque<>();

  private final List<PathState> matchResult = new ArrayList<>();

  public RegexMatchState(Section parentSectionNow) {
    this.patternSectionNow = parentSectionNow;
  }

  public List<PathState> getMatchResult() {
    return matchResult;
  }

  public List<Section> getDataSectionList() {
    return dataSectionList;
  }

  private void calcMatchValue(PathState pathState, double smoothValue, double threshold) {
    // calc the matchValue between sectionStack and dataSectionList, meta info is in pathState
    int index = 0;
    Iterator<Section> iterator = sectionStack.descendingIterator();
    pathState.calcGlobalRadio(smoothValue);
    while (iterator.hasNext()) {
      Section patternSection = iterator.next();
      Section dataSection = dataSectionList.get(index);
      if (pathState.calcOneSectionMatchValue(patternSection, dataSection, smoothValue, threshold)) {
        break;
      }
      index++;
    }
    // if get out over threshold, there is a index++ less than normal case
    if (index == dataSectionList.size()) {
      matchResult.add(pathState);
    }
  }

  // return true claim that the pathState is finished, false claim that the pathState is alive
  public Boolean checkOneSectionInTopPathState(
      Section section,
      double heightLimit,
      double widthLimit,
      double smoothValue,
      double threshold) {
    // scan a start in one loop, and record the calc result in each section, and record the match
    // path in each state

    // find the path match the sign in the section which not more than shapeTolerance

    // scan the dataSectionList. if the section pair no calc before, need to calc first and store.
    // else the calc result is calc before, only need to update the Gx,Gy,Hc
    // while accumulate the result, need to check whether the result is in the threshold, if not,
    // cut the branch and throw the result

    // the result only record the calc result distance, the start section, the length of the
    // resultSectionList

    PathState pathState = matchStateStack.pop();
    sectionStack.push(pathState.getPatternSection());

    if (pathState.checkSign(section) && checkBoundLimit(pathState, heightLimit, widthLimit)) {
      if (pathState.getPatternSection().isFinal()) {
        calcMatchValue(pathState, smoothValue, threshold);
      }
      if (section.isFinal()) {
        return true; // if the section is final, claim that the pathState is finished
      }

      // trans to next state
      if (!pathState.getPatternSection().getNextSectionList().isEmpty()) {
        if (pathState.getPatternSection().getNextSectionList().size() == 1) {
          Section nextSection = pathState.getPatternSection().getNextSectionList().get(0);
          if (pathState.getPatternSection().isFinal()) { // 这里不能在之前的上面继续改，这里需要保留这个index，之后输出要用
            PathState newPathState =
                new PathState(pathState.getDataSectionIndex() + 1, nextSection, pathState);
            matchStateStack.push(newPathState);
          } else {
            pathState.nextState(nextSection);
            matchStateStack.push(pathState);
          }
        } else {
          // copy the old pathState info and new dataSectionIndex, patternSection
          // loop from the last one, the first one can be the top of the stack
          for (int i = pathState.getPatternSection().getNextSectionList().size() - 1; i >= 0; i--) {
            PathState newPathState =
                new PathState(
                    pathState.getDataSectionIndex() + 1,
                    pathState.getPatternSection().getNextSectionList().get(i),
                    pathState);
            matchStateStack.push(newPathState);
          }
        }
        return false;
      }
    }
    return true;
  }

  // return true claim that the RegexMatchState is finish , false claim that regexMatchState is
  // alive and wait for next section
  public Boolean addSection(
      Section section,
      double heightLimit,
      double widthLimit,
      double smoothValue,
      double threshold) {
    dataSectionList.add(section);
    if (matchStateStack.isEmpty()) {
      if (patternSectionNow.getSign() == 2) {
        for (Section startSection : patternSectionNow.getNextSectionList()) {
          matchStateStack.push(new PathState(0, startSection));
        }
      } else {
        matchStateStack.push(new PathState(0, patternSectionNow));
      }
    }

    if (checkOneSectionInTopPathState(section, heightLimit, widthLimit, smoothValue, threshold)) {
      while (!matchStateStack.isEmpty()
          && matchStateStack.peek() != null
          && matchStateStack.peek().getDataSectionIndex() < dataSectionList.size()) {
        // update the top one's state
        PathState topPathState = matchStateStack.peek();
        // lose the top of the section until sectionStack size is smaller than the dataSectionIndex
        // of the pathState
        while (topPathState.getDataSectionIndex() < sectionStack.size()
            && !sectionStack.isEmpty()) {
          sectionStack.pop();
        }
        // loop the dataSectionList from the dataSectionIndex to the end
        for (int i = topPathState.dataSectionIndex; i < dataSectionList.size(); i++) {
          if (checkOneSectionInTopPathState(
              dataSectionList.get(i), heightLimit, widthLimit, smoothValue, threshold)) {
            break;
          }
        }
      }
    }

    // claim that start from this section has no case and run continue return true to trans to the
    // next start.
    // while have some pathState in the stack, check the top one whether.
    // it's index point to the next of the end section of
    // dataSectionList(matchStateStack.peek().getDataSectionIndex() == dataSectionList.size())
    // ,claim it is wait for next section
    return matchStateStack.isEmpty();
  }

  private Boolean checkBoundLimit(PathState pathState, double heightLimit, double widthLimit) {
    return (pathState.getDataHeightBound() <= heightLimit)
        && (pathState.getDataWidthBound() <= widthLimit);
  }

  public static class PathState {
    private double matchValue = 0.0;

    private int dataSectionIndex = 0;
    private Section patternSection = null;

    private double dataMaxHeight = 0.0;
    private double dataMinHeight = Double.MAX_VALUE;
    private double dataMaxWidth = 0.0;
    private double dataMinWidth = Double.MAX_VALUE;

    private double patternMaxHeight = 0.0;
    private double patternMinHeight = Double.MAX_VALUE;
    private double patternMaxWidth = 0.0;
    private double patternMinWidth = Double.MAX_VALUE;

    private int shapeNotMatch = 0;

    // this is the Gx and Gy in the paper
    private double globalWitdhRadio = 0.0;
    private double globalHeightRadio = 0.0;

    public PathState(int dataSectionIndex, Section patternSection) {
      this.dataSectionIndex = dataSectionIndex;
      this.patternSection = patternSection;
      updatePatternBounds(patternSection);
    }

    public PathState(int dataSectionIndex, Section patternSection, PathState pathState) {
      copy(pathState);
      this.dataSectionIndex = dataSectionIndex;
      this.patternSection = patternSection;
      updatePatternBounds(patternSection);
    }

    public void nextState(Section patternSection) {
      this.dataSectionIndex += 1;
      this.patternSection = patternSection;
      updatePatternBounds(patternSection);
    }

    public Section getPatternSection() {
      return patternSection;
    }

    public int getDataSectionIndex() {
      return dataSectionIndex;
    }

    public Boolean checkSign(Section section) {
      if (section.getSign() == patternSection.getSign()) {
        updateDataBounds(section);
        return true;
      }
      shapeNotMatch++;
      if (shapeNotMatch <= SHAPE_TOLERANCE) {
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

    private void copy(PathState pathState) {
      this.dataMaxHeight = pathState.dataMaxHeight;
      this.dataMinHeight = pathState.dataMinHeight;
      this.dataMaxWidth = pathState.dataMaxWidth;
      this.dataMinWidth = pathState.dataMinWidth;

      this.patternMaxHeight = pathState.patternMaxHeight;
      this.patternMinHeight = pathState.patternMinHeight;
      this.patternMaxWidth = pathState.patternMaxWidth;
      this.patternMinWidth = pathState.patternMinWidth;
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
      globalWitdhRadio = (dataMaxWidth - dataMinWidth) / (patternMaxWidth - patternMinWidth);
    }

    public Boolean calcOneSectionMatchValue(
        Section patternSection, Section dataSection, double smoothValue, double threshold) {
      // calc the LED
      // this is the Rx and Ry in the paper
      double localWidthRadio =
          dataSection.getWidthBound() / (patternSection.getWidthBound() * globalWitdhRadio);

      double localHeightUp = Math.max(dataSection.getHeightBound(), smoothValue);
      double localHeightDown =
          Math.max(patternSection.getHeightBound() * globalHeightRadio, smoothValue);
      double localHeightRadio = localHeightUp / localHeightDown;

      double led = Math.pow(Math.log(localWidthRadio), 2) + Math.pow(Math.log(localHeightRadio), 2);

      // different way
      double shapeError = 0.0;
      if (CALC_SE_USING_MORE_MEMORY
          && dataSection.getCalcResult().get(patternSection.getId()) != null) {
        shapeError =
            dataSection.getCalcResult().get(patternSection.getId())
                / ((dataMaxHeight - dataMinHeight) == 0
                    ? smoothValue
                    : (dataMaxHeight - dataMinHeight));
      } else {
        // calc the SE
        // align the first point or the centroid, it's same because the calculation is just an avg
        // function, no matter where the align point is
        double alignHeightDiff =
            patternSection.getPoints().get(0).y * globalHeightRadio * localHeightRadio
                - dataSection.getPoints().get(0).y;

        // different from the origin code, in this case this code use linear fill technology to
        // align the x-axis of all point in patternSection to dataSection
        int dataPointNum = dataSection.getPoints().size() - 1;
        int patternPointNum = patternSection.getPoints().size() - 1;

        double numRadio = ((double) patternPointNum) / ((double) dataPointNum);

        for (int i = 1; i < dataPointNum; i++) {
          double patternIndex = i * numRadio;
          int leftIndex = (int) patternIndex;
          double leftRadio = patternIndex - leftIndex;
          int rightIndex = leftIndex >= patternPointNum ? patternPointNum : leftIndex + 1;
          double rightRadio = 1 - leftRadio;
          // the heightValue is a weighted avg about the index near num
          double pointHeight =
              patternSection.getPoints().get(leftIndex).y * rightRadio
                  + patternSection.getPoints().get(rightIndex).y * leftRadio;

          shapeError +=
              Math.abs(
                  pointHeight * globalHeightRadio * localHeightRadio
                      - alignHeightDiff
                      - dataSection.getPoints().get(i).y);
        }

        shapeError =
            shapeError
                / (((dataMaxHeight - dataMinHeight) == 0
                        ? smoothValue
                        : (dataMaxHeight - dataMinHeight))
                    * (dataSection.getPoints().size() - 1));

        if (CALC_SE_USING_MORE_MEMORY) {
          dataSection
              .getCalcResult()
              .put(
                  patternSection.getId(),
                  shapeError
                      * (((dataMaxHeight - dataMinHeight) == 0
                          ? smoothValue
                          : (dataMaxHeight - dataMinHeight))));
        }
      }

      matchValue = matchValue + led + shapeError;
      return matchValue > threshold;
    }

    public double getMatchValue() {
      return matchValue;
    }
  }
}
