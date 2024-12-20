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

package org.apache.iotdb.library.match;

import org.apache.iotdb.library.match.model.Bounds;
import org.apache.iotdb.library.match.model.PatternCalculationResult;
import org.apache.iotdb.library.match.model.PatternContext;
import org.apache.iotdb.library.match.model.PatternResult;
import org.apache.iotdb.library.match.model.Point;
import org.apache.iotdb.library.match.model.Section;
import org.apache.iotdb.library.match.model.SectionCalculation;
import org.apache.iotdb.library.match.model.SectionNext;
import org.apache.iotdb.library.match.utils.LinearScale;
import org.apache.iotdb.library.match.utils.TimeScale;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

public class PatternExecutor {
  List<Point> points = new ArrayList<>(); // all the extracted point of the query
  List<Double> tangents = new ArrayList<>(); // all the tangents of the query
  List<Section> sections = new ArrayList<>(); // all the sections of the query

  Long queryLength = null;
  Long queryLengthTolerance = null;
  Long queryHeight = null;
  Long queryHeightTolerance = null;

  public List<Point> extractPoints(Long[] times, Double[] values) {
    List<Point> points = new ArrayList<>();

    double px = times[0];
    for (int i = 0; i < times.length; i += 1) {
      if (times[i] >= px) {
        points.add(new Point(times[i], values[i], times[i], values[i]));
        px += 1;
      }
    }

    // translate the query to have the minimum y to 0
    double originY = points.stream().map(Point::getY).min(Double::compare).orElseGet(() -> 0.0);
    // translate the query to have the minimum x to 0
    double originX = points.stream().map(Point::getX).min(Double::compare).orElseGet(() -> 0.0);

    for (int i = 0; i < points.size(); i += 1) {
      Point pt = points.get(i);
      pt.setY(pt.getY() - originY);
      pt.setX(pt.getX() - originX);
    }
    return points;
  }

  /**
   * query Points
   *
   * @param sourcePoints
   * @return
   */
  public List<Point> extractPoints(List<Point> sourcePoints) {
    List<Point> points = new ArrayList<>();
    double px, originY, originX;
    Point p;
    int i;

    px = sourcePoints.get(0).getX();
    for (i = 0; i < sourcePoints.size(); i += 1) {
      p = sourcePoints.get(i);
      if (p.getX() >= px) {
        points.add(new Point(p.getX(), p.getY(), p.getX(), p.getY()));
        px += 1;
      }
    }

    // flip y because in the query paper the point (0,0) is in the left-top corner
    originY = points.stream().map(Point::getY).max(Double::compare).orElseGet(() -> 0.0);
    // translate the query to have the minimum x to 0
    originX = points.stream().map(Point::getX).min(Double::compare).orElseGet(() -> 0.0);

    for (i = 0; i < points.size(); i += 1) {
      Point pt = points.get(i);
      pt.setY(pt.getY());
      pt.setX(pt.getX() - originX);
    }
    return points;
  }

  public void setPoints(List<Point> points) {
    this.points = points;
    this.tangents = extractTangents(points);
    this.sections =
        findCurveSections(tangents, points, PatternMatchConfig.DIVIDE_SECTION_MIN_HEIGHT_QUERY);
  }

  public List<Point> scalePoint(List<Long> times, List<Double> values) {
    List<Point> points = new ArrayList<>();
    TimeScale xScale = new TimeScale(times.get(0), times.get(times.size() - 1), 0, 1000);
    double minY = values.stream().min(Double::compare).orElseGet(() -> 0.0);
    double maxY = values.stream().max(Double::compare).orElseGet(() -> 0.0);
    LinearScale<Double> yScale = new LinearScale<>(minY, maxY, 0, 500);
    for (int i = 0; i < times.size(); i += 1) {
      points.add(
          new Point(
              xScale.scale(times.get(i)),
              yScale.scale(values.get(i)),
              times.get(i),
              values.get(i)));
    }
    return points;
  }

  public List<Point> scalePoint(List<Point> sourcePoints) {
    List<Point> points = new ArrayList<>();
    TimeScale xScale =
        new TimeScale(
            (long) sourcePoints.get(0).getX(),
            (long) sourcePoints.get(sourcePoints.size() - 1).getX(),
            0,
            1000);
    double minY = sourcePoints.stream().map(Point::getY).min(Double::compare).orElseGet(() -> 0.0);
    double maxY = sourcePoints.stream().map(Point::getY).max(Double::compare).orElseGet(() -> 0.0);
    LinearScale<Double> yScale = new LinearScale<>(minY, maxY, 0, 500);
    for (int i = 0; i < sourcePoints.size(); i += 1) {
      Point p = sourcePoints.get(i);
      points.add(
          new Point(xScale.scale((long) p.getX()), yScale.scale(p.getY()), p.getX(), p.getY()));
    }
    return points;
  }

  public List<PatternResult> executeQuery(PatternContext queryCtx) {
    executeQueryInSI(queryCtx);
    return queryCtx.getMatches();
  }

  /** Execute the query in a particular smooth iteration */
  private void executeQueryInSI(PatternContext queryCtx) {
    int dsi = 0;
    if (queryCtx.getDatasetSize() == null) {
      queryCtx.setDatasetSize(
          queryCtx.getDataPoints().get(queryCtx.getDataPoints().size() - 1).getX()
              - queryCtx.getDataPoints().get(0).getX());
    }
    List<Double> dataTangents = extractTangents(queryCtx.getDataPoints());
    List<Section> dataSections =
        findCurveSections(
            dataTangents,
            queryCtx.getDataPoints(),
            PatternMatchConfig.DIVIDE_SECTION_MIN_HEIGHT_DATA);
    for (dsi = 0; dsi < dataSections.size(); dsi++) {
      for (int i = 0; i < this.sections.size(); i++) {
        for (int j = 0; j < this.sections.get(i).getNext().size(); j++) {
          this.sections.get(i).getNext().get(j).setTimes(1);
        }
      }
      if (!matchIn(
          this.sections.get(0),
          dataSections,
          dsi,
          new ArrayList<>(),
          queryCtx,
          this.sections.get(this.sections.size() - 1))) {
        break;
      }
    }
  }

  private double tangent(Point p1, Point p2) {
    return (p2.getY() - p1.getY()) / (p2.getX() - p1.getX());
  }

  /**
   * Extract the tangents of a set of points
   *
   * @return *[] array of tangents: the first tangent is related to the first and second point the
   *     second is related to the second and the first the third is related to the third and the
   *     second ...
   */
  private List<Double> extractTangents(List<Point> points) {
    if (points.size() < 2) {
      return new ArrayList<>();
    }
    List<Double> tangents = new ArrayList<>();
    tangents.add(tangent(points.get(0), points.get(1)));
    for (int i = 1; i < points.size(); i++) {
      tangents.add(tangent(points.get(i - 1), points.get(i)));
    }
    return tangents;
  }

  /**
   * last x - first x
   *
   * @param points
   * @return
   */
  private double calcWidth(List<Point> points) {
    return points.get(points.size() - 1).getX() - points.get(0).getX();
  }

  /**
   * max y - min y
   *
   * @param points
   * @return
   */
  private double calcHeight(List<Point> points) {
    return points.stream().map(Point::getY).max(Double::compare).orElseGet(() -> 0.0)
        - points.stream().map(Point::getY).min(Double::compare).orElseGet(() -> 0.0);
  }

  /**
   * Given a list of tangents it divides the list of tangents in a list of sections. Each section
   * shares the same tangent signs (a section could be an increasing curve or a decreasing curve,
   * but not both)
   *
   * @return Array of sections.
   */
  private List<Section> findCurveSections(
      List<Double> tangents, List<Point> points, double minHeightPerc) {
    List<Section> sections = new ArrayList<>();
    Double lastTg = null;
    Point lastPt = null;
    double totalHeight = calcHeight(points);
    double lastSectHeight = 0;
    for (int i = 0; i < tangents.size(); i++) {
      Double tangent = tangents.get(i);
      Point pt = points.get(i);
      double sign = Math.signum(tangent);

      if (sections.size() == 0) {
        sections.add(new Section(sign));
      } else if (sign != 0) {
        Section lastSect = sections.get(sections.size() - 1);
        if (lastSect.getSign() != sign) {
          lastSectHeight = calcHeight(lastSect.getPoints());
          if (lastSect.getPoints().size() > 1
              && (!(minHeightPerc > 0) || lastSectHeight / totalHeight > minHeightPerc)) {
            Section newSection = new Section(sign);
            sections.add(newSection);
            newSection.getPoints().add(lastPt);
            newSection.getTangents().add(lastTg);
          }
        }
      }

      Section lastSect = sections.get(sections.size() - 1);
      lastSect.getPoints().add(pt);
      lastSect.getTangents().add(tangent);
      lastTg = tangent;
      lastPt = pt;
    }

    int count = 0;
    Section prev = null;
    for (Section s : sections) {
      s.setId(count++);
      if (prev != null) {
        prev.getNext().add(new SectionNext(s));
      }
      prev = s;
    }
    prev.setNext(new ArrayList<>());

    return sections;
  }

  private boolean matchIn(
      Section currSect,
      List<Section> dataSections,
      int dsi,
      List<Section> qSections,
      PatternContext queryCtx,
      Section lastQuerySect) {
    if (qSections.size() > PatternMatchConfig.MAX_REGEX_IT) {
      return false;
    }
    PatternResult matchValue = null;
    int i = 0;
    List<Section> sectsBlock = new ArrayList<>();
    sectsBlock.add(currSect);

    // Translate the query that is always in a regexp-form (even when there are no repetitions) in
    // an array of sections
    // This until there is only one next element
    while (currSect.getNext().size() == 1 && currSect != lastQuerySect) {
      currSect = currSect.getNext().get(0).getDest();
      sectsBlock.add(currSect);
    }

    if (dsi + sectsBlock.size() + qSections.size() > dataSections.size()) {
      return false; // the next group is too long for the remaining data sections
    }

    // translate the new sections in case of repetitions
    if (qSections.size() > 0) {
      /* TODO if slow could be useful to have a cache (the key could be based on the sections id) to avoid those copies */
      Point lastQSectionsSectPt =
          qSections
              .get(qSections.size() - 1)
              .getPoints()
              .get(qSections.get(qSections.size() - 1).getPoints().size() - 1);
      Point firstSectsBlockPt = sectsBlock.get(0).getPoints().get(0);
      if (firstSectsBlockPt.getX() < lastQSectionsSectPt.getX()) {
        double offset = -firstSectsBlockPt.getX() + lastQSectionsSectPt.getX();
        double offseto = -firstSectsBlockPt.getOrigX() + lastQSectionsSectPt.getOrigX();
        for (i = 0; i < sectsBlock.size(); i++) {
          sectsBlock.set(i, sectsBlock.get(i).translateXCopy(offset, offseto));
        }
      }
    }
    List<Section> newQSections = new ArrayList<>(qSections);
    newQSections.addAll(sectsBlock);

    List<Section> dataSectsForQ = dataSections.subList(dsi, dsi + newQSections.size());

    // If we reached the end of the query we can actually use it
    if (currSect == lastQuerySect
        && (currSect.getNext().size() == 0
            || currSect.getNext().get(0).getSize() != 0
            || currSect.getNext().get(0).getSize() == currSect.getNext().get(0).getTimes())) {
      matchValue = this.calculateMatch(dataSectsForQ, newQSections, queryCtx, false);
      if (matchValue != null) {

        // Keep only one (best) match if the same area is selected in different smooth iterations
        int duplicateMatchIdx =
            PatternMatchConfig.REMOVE_EQUAL_MATCHES
                ? this.searchEqualMatch(matchValue, queryCtx.getMatches())
                : -1;
        if (duplicateMatchIdx == -1) {
          matchValue.setId(queryCtx.getMatches().size()); // new id for the new match
          queryCtx.getMatches().add(matchValue);
        } else if (queryCtx.getMatches().get(duplicateMatchIdx).getMatch()
            > matchValue.getMatch()) {
          matchValue.setId(
              queryCtx
                  .getMatches()
                  .get(duplicateMatchIdx)
                  .getId()); // we leave the old id for the match
          queryCtx.getMatches().set(duplicateMatchIdx, matchValue);
        }
      }
    }

    if (!currSect.getNext().isEmpty()) {
      boolean backLink = false;
      for (i = currSect.getNext().size() - 1;
          i >= 0;
          i--) { // iterate repetitions and after the straight link
        SectionNext next = currSect.getNext().get(i);
        if (currSect == lastQuerySect || i > 0) { // it is a back link
          if (next.getSize() != 0) {
            this.matchIn(next.getDest(), dataSections, dsi, newQSections, queryCtx, lastQuerySect);
          } else if (next.getTimes() < next.getSize()) {
            next.setTimes(next.getTimes() + 1);
            backLink = true; // exclude the straight link only if there is a strict repetition
            this.matchIn(next.getDest(), dataSections, dsi, newQSections, queryCtx, lastQuerySect);
          }
        } else if (!backLink) {
          this.matchIn(next.getDest(), dataSections, dsi, newQSections, queryCtx, lastQuerySect);
        }
      }
    }
    return true;
  }

  private PatternResult calculateMatch(
      List<Section> matchedSections,
      List<Section> querySections,
      PatternContext queryCtx,
      boolean partialQuery) {
    PatternCalculationResult pointsMatchRes =
        calculatePointsMatch(querySections, matchedSections, partialQuery);
    if (pointsMatchRes == null) {
      return null;
    }
    if (pointsMatchRes.getMatch() > queryCtx.getThreshold()) {
      return null;
    }
    if (partialQuery) {
      PatternResult result = new PatternResult();
      result.setMatch(pointsMatchRes.getMatch());
      return result;
    }

    List<Point> matchedPts = pointsMatchRes.getMatchedPoints();
    double minPos = matchedPts.get(0).getX();
    double maxPos = matchedPts.get(matchedPts.size() - 1).getX();
    double matchSize = (maxPos - minPos) / queryCtx.getDatasetSize();
    double matchPos = ((maxPos + minPos) / 2) / queryCtx.getDatasetSize();
    Long matchTimeSpan =
        Math.round(matchedPts.get(matchedPts.size() - 1).getOrigX() - matchedPts.get(0).getOrigX());

    if (this.queryHeight != null) {
      if (!checkQueryHeight(calculateMatchHeight(matchedPts))) {
        return null;
      }
    }

    PatternResult result = new PatternResult();
    result.setMatch(pointsMatchRes.getMatch());
    result.setPoints(matchedPts);
    result.setSize(matchSize);
    result.setMatchPos(matchPos);
    result.setTimespan(matchTimeSpan);
    result.setMinPos(minPos);
    result.setMaxPos(maxPos);
    result.setSections(matchedSections);

    return result;
  }

  /* Calculate the match considering comparing the given sections to all the sections of the query.
   * Each query section is scaled to match each section of the argument, and its tangents are compared. */
  private PatternCalculationResult calculatePointsMatch(
      List<Section> querySections, List<Section> matchedSections, boolean partialQuery) {
    if (PatternMatchConfig.CHECK_QUERY_COMPATIBILITY) {
      if (!areCompatibleSections(querySections, matchedSections, !partialQuery)) {
        return null;
      }
    } else {
      if (querySections.size() > matchedSections.size()) {
        matchedSections = expandSections(matchedSections, querySections.size());
      } else if (querySections.size() < matchedSections.size()) {
        matchedSections = reduceSections(matchedSections, querySections.size());
      }
      if (matchedSections == null) {
        return null;
      }
      if (!areCompatibleSections(querySections, matchedSections, !partialQuery)) {
        return null;
      }
    }

    double centroidsDifference;
    int i, si;

    Bounds matchedSecBounds =
        getBounds(
            matchedSections,
            (matchedSections.size() > 2 ? 1 : 0),
            matchedSections.size() - (matchedSections.size() > 2 ? 2 : 1));

    Bounds queryBounds =
        getBounds(
            querySections,
            (querySections.size() > 2 ? 1 : 0),
            querySections.size() - (querySections.size() > 2 ? 2 : 1));

    double subSequenceScaleFactorX =
        (matchedSecBounds.getMaxX() - matchedSecBounds.getMinX())
            / (queryBounds.getMaxX() - queryBounds.getMinX());
    double subSequenceScaleFactorY =
        (matchedSecBounds.getMaxY() - matchedSecBounds.getMinY())
            / (queryBounds.getMaxY() - queryBounds.getMinY());

    List<Point> matchedPoints = new ArrayList<>();
    double pointDifferencesCost = 0;
    double rescalingCost = 0;
    double sum = 0;
    double num = 0;

    for (si = 0; si < querySections.size(); si++) {
      SectionCalculation dataSect = new SectionCalculation();
      SectionCalculation querySect = new SectionCalculation();
      sum = 0;
      num = 0;

      querySect.setPoints(querySections.get(si).getPoints());
      querySect.setWidth(calcWidth(querySect.getPoints()));
      querySect.setHeight(calcHeight(querySect.getPoints()));
      if (querySect.getHeight() == 0) {
        continue;
      }

      if (si == 0 && querySections.size() > 2 && PatternMatchConfig.START_END_CUT_IN_SUBPARTS) {
        dataSect.setPoints(
            sectionEndSubpartPoints(
                matchedSections.get(si), querySect.getWidth() * subSequenceScaleFactorX));
      } else if (si == querySections.size() - 1
          && querySections.size() > 2
          && PatternMatchConfig.START_END_CUT_IN_SUBPARTS_IN_RESULTS) {
        dataSect.setPoints(
            sectionStartSubpartPoints(
                matchedSections.get(si), querySect.getWidth() * subSequenceScaleFactorX));
      } else {
        dataSect.setPoints(matchedSections.get(si).getPoints());
      }
      dataSect.setWidth(calcWidth(dataSect.getPoints()));
      dataSect.setHeight(calcHeight(dataSect.getPoints()));
      if (dataSect.getHeight() == 0) {
        continue;
      }

      double scaleFactorX = dataSect.getWidth() / (querySect.getWidth() * subSequenceScaleFactorX);
      double scaleFactorY =
          dataSect.getHeight()
              / (querySect.getHeight()
                  * (PatternMatchConfig.RESCALING_Y
                      ? subSequenceScaleFactorY
                      : subSequenceScaleFactorX));

      if (scaleFactorX != 0 && scaleFactorY != 0) {
        rescalingCost +=
            (Math.pow(Math.log(scaleFactorX), 2) + Math.pow(Math.log(scaleFactorY), 2));
      }

      // calculate the centroid of the two sections to align them
      dataSect.setCentroidY(0);
      for (i = 0; i < dataSect.getPoints().size(); i++) {
        dataSect.setCentroidY(dataSect.getCentroidY() + dataSect.getPoints().get(i).getY());
      }
      dataSect.setCentroidY(dataSect.getCentroidY() / dataSect.getPoints().size());
      querySect.setCentroidY(0);
      for (i = 0; i < querySect.getPoints().size(); i++) {
        querySect.setCentroidY(
            querySect.getPoints().get(0).getY()
                * (PatternMatchConfig.RESCALING_Y
                    ? subSequenceScaleFactorY
                    : subSequenceScaleFactorX)
                * scaleFactorY);
      }
      querySect.setCentroidY(querySect.getCentroidY() / querySect.getPoints().size());
      centroidsDifference =
          querySect.getPoints().get(0).getY()
                  * (PatternMatchConfig.RESCALING_Y
                      ? subSequenceScaleFactorY
                      : subSequenceScaleFactorX)
                  * scaleFactorY
              - dataSect.getPoints().get(0).getY();

      double queryPtsStep = (double) querySect.getPoints().size() / dataSect.getPoints().size();

      for (i = 0; i < dataSect.getPoints().size(); i++) {
        Point dataPt = dataSect.getPoints().get(i);
        Point queryPt = querySect.getPoints().get((int) Math.floor(i * queryPtsStep));

        double resSum =
            Math.abs(
                    (queryPt.getY()
                                * (PatternMatchConfig.RESCALING_Y
                                    ? subSequenceScaleFactorY
                                    : subSequenceScaleFactorX)
                                * scaleFactorY
                            - centroidsDifference)
                        - dataPt.getY())
                / dataSect.getHeight();
        sum += resSum;
        num++;
      }

      if (!partialQuery) {
        if (PatternMatchConfig.START_END_CUT_IN_SUBPARTS_IN_RESULTS) {
          for (i = 0; i < dataSect.getPoints().size(); i++) {
            matchedPoints.add(dataSect.getPoints().get(i));
          }
        } else {
          for (i = 0; i < matchedSections.get(si).getPoints().size(); i++) {
            matchedPoints.add(matchedSections.get(si).getPoints().get(i));
          }
        }
      }

      if (num > 0) {
        pointDifferencesCost += sum / num;
      }
    }
    PatternCalculationResult result = new PatternCalculationResult();
    result.setMatch(
        pointDifferencesCost * PatternMatchConfig.VALUE_DIFFERENCE_WEIGHT
            + rescalingCost * PatternMatchConfig.RESCALING_COST_WEIGHT);
    result.setMatchedPoints(matchedPoints);
    return result;
  }

  // SectionStartSubpartPoints
  public List<Point> sectionStartSubpartPoints(Section section, double width) {
    List<Point> points = new ArrayList<>();
    double startX = section.getPoints().get(0).getX();
    for (int pi = 0; pi < section.getPoints().size(); pi++) {
      points.add(section.getPoints().get(pi));
      if (section.getPoints().get(pi).getX() - startX >= width) {
        break;
      }
    }
    return points;
  }

  // SectionEndSubpartPoints
  public List<Point> sectionEndSubpartPoints(Section section, double width) {
    List<Point> points = new ArrayList<>();
    double endX = section.getPoints().get(section.getPoints().size() - 1).getX();
    for (int pi = section.getPoints().size() - 1; pi >= 0; pi--) {
      points.add(0, section.getPoints().get(pi));
      if (endX - section.getPoints().get(pi).getX() >= width) {
        break;
      }
    }
    return points;
  }

  // CheckQueryLength
  public boolean checkQueryLength(double queryLength) {
    if (this.queryLength == null) {
      return true;
    }
    double min = this.queryLength - this.queryLength * this.queryLengthTolerance;
    double max = this.queryLength + this.queryLength * this.queryLengthTolerance;
    return queryLength >= min && queryLength <= max;
  }

  // CheckQueryHeight
  public boolean checkQueryHeight(double queryHeight) {
    if (this.queryHeight == null) {
      return true;
    }
    double min = this.queryHeight - this.queryHeight * this.queryHeightTolerance;
    double max = this.queryHeight + this.queryHeight * this.queryHeightTolerance;
    return queryHeight >= min && queryHeight <= max;
  }

  // CalculateMatchHeight
  public double calculateMatchHeight(List<Point> matchedPts) {
    double minY = Collections.min(matchedPts, Comparator.comparingDouble(Point::getY)).getOrigY();
    double maxY = Collections.max(matchedPts, Comparator.comparingDouble(Point::getY)).getOrigY();
    return maxY - minY;
  }

  // SearchEqualMatch
  public int searchEqualMatch(PatternResult targetMatch, List<PatternResult> matches) {
    double targetStartX = targetMatch.getPoints().get(0).getX();
    double targetEndX = targetMatch.getPoints().get(targetMatch.getPoints().size() - 1).getX();
    for (int idx = 0; idx < matches.size(); idx++) {
      if (Math.abs(targetStartX - matches.get(idx).getPoints().get(0).getX()) <= 10
          && Math.abs(
                  targetEndX
                      - matches
                          .get(idx)
                          .getPoints()
                          .get(matches.get(idx).getPoints().size() - 1)
                          .getX())
              <= 10) {
        return idx;
      }
    }
    return -1;
  }

  // AreCompatibleSections
  public boolean areCompatibleSections(
      List<Section> querySections, List<Section> dataSections, boolean checkLength) {
    if (querySections.size() != dataSections.size()) {
      return false;
    }

    if (this.queryLength != null && checkLength) {
      Section lastDataSection = dataSections.get(dataSections.size() - 1);
      double maxMatchLength =
          lastDataSection.getPoints().get(lastDataSection.getPoints().size() - 1).getOrigX()
              - dataSections.get(0).getPoints().get(0).getOrigX()
              + this.queryLength * this.queryLengthTolerance;
      double minMatchLength =
          (dataSections.size() == 1
                  ? 0
                  : lastDataSection.getPoints().get(0).getOrigX()
                      - dataSections
                          .get(0)
                          .getPoints()
                          .get(dataSections.get(0).getPoints().size() - 1)
                          .getOrigX())
              - this.queryLength * this.queryLengthTolerance;
      if (this.queryLength > maxMatchLength || this.queryLength < minMatchLength) {
        return false;
      }
    }

    double incompatibleSections = 0;
    for (int j = 0; j < querySections.size(); j++) {
      if (querySections.get(j).getSign() != 0
          && querySections.get(j).getSign() != dataSections.get(j).getSign()) {
        incompatibleSections++;
      }
    }
    return incompatibleSections / querySections.size()
        <= PatternMatchConfig.QUERY_SIGN_MAXIMUM_TOLERABLE_DIFFERENT_SIGN_SECTIONS;
  }

  // GetBounds
  public Bounds getBounds(List<Section> sections, int startSectIdx, int endSectIdx) {
    if (sections == null) {
      return null;
    }
    Bounds bounds = new Bounds();
    bounds.setMinX(sections.get(startSectIdx).getPoints().get(0).getX());
    bounds.setMaxX(
        sections
            .get(endSectIdx)
            .getPoints()
            .get(sections.get(endSectIdx).getPoints().size() - 1)
            .getX());
    for (int i = startSectIdx; i < endSectIdx; i++) {
      Stream<Double> yList = sections.get(i).getPoints().stream().map(Point::getY);
      double localMinY =
          sections.get(i).getPoints().stream()
              .map(Point::getY)
              .min(Double::compare)
              .orElseGet(() -> (double) Long.MAX_VALUE);
      double localMaxY =
          sections.get(i).getPoints().stream()
              .map(Point::getY)
              .max(Double::compare)
              .orElseGet(() -> (double) Long.MAX_VALUE);
      if (localMinY < bounds.getMinY()) {
        bounds.setMinY(localMinY);
      }
      if (localMaxY > bounds.getMaxY()) {
        bounds.setMaxY(localMaxY);
      }
    }
    return bounds;
  }

  /**
   * reduce the number of sections to n, joining the smallest sections to the smallest adjacent
   *
   * @param sections
   * @param n
   * @return
   */
  public List<Section> reduceSections(List<Section> sections, int n) {
    if (n >= sections.size() || n < 1) {
      return sections;
    }
    List<Section> newSections = new ArrayList<>(sections);
    while (n < newSections.size()) {
      Integer smallestSection = null;
      double sectionSizeAvg = 0;
      for (int i = 0; i < newSections.size(); i++) {
        sectionSizeAvg += newSections.get(i).sizeEucl();
        if (smallestSection == null
            || newSections.get(smallestSection).sizeEucl() > newSections.get(i).sizeEucl()) {
          smallestSection = i;
        }
      }
      sectionSizeAvg /= newSections.size();
      if (newSections.get(smallestSection).sizeEucl() > sectionSizeAvg * 0.8) {
        return null;
      }

      if (smallestSection == 0) {
        newSections.get(0).concat(newSections.get(1));
        newSections.remove(1);
      } else if (smallestSection == newSections.size() - 1) {
        newSections.get(newSections.size() - 2).concat(newSections.get(newSections.size() - 1));
        newSections.remove(newSections.size() - 1);
      } else if (newSections.get(smallestSection - 1).sizeEucl()
          <= newSections.get(smallestSection + 1).sizeEucl()) {
        newSections.get(smallestSection - 1).concat(newSections.get(smallestSection));
        newSections.remove(newSections.get(smallestSection));
      } else {
        newSections.get(smallestSection).concat(newSections.get(smallestSection + 1));
        newSections.remove(newSections.get(smallestSection + 1));
      }
    }
    return newSections;
  }

  // ExpandSections
  public List<Section> expandSections(List<Section> sections, int n) {
    if (n <= sections.size()) {
      return sections;
    }
    List<Section> newSections = new ArrayList<>(sections);
    for (int i = sections.size(); i <= n; i++) {
      newSections.add(sections.get(sections.size() - 1));
    }
    return newSections;
  }
}
