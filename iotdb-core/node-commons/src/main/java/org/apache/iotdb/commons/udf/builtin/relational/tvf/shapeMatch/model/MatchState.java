package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

import java.util.ArrayList;
import java.util.List;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.shapeTolerance;

public class MatchState {
  private double matchValue = 0.0;
  private Boolean isFinish = false;

  private List<Section> dataSectionList = new ArrayList<>();

  private List<Section> sections = new ArrayList<>();

  private Section patternSectionNow = null;

  private int startSectionId = -1;

  private int shapeError = 0;

  private double TotalUpHeight = 0.0;

  private double dataMaxHeight = 0.0;
  private double dataMinHeight = Double.MAX_VALUE;
  private double dataMaxWidth = 0.0;
  private double dataMinWidth = Double.MAX_VALUE;

  private double patternMaxHeight = 0.0;
  private double patternMinHeight = Double.MAX_VALUE;
  private double patternMaxWidth = 0.0;
  private double patternMinWidth = Double.MAX_VALUE;

  // this is the Gx and Gy in the paper
  private double globalWitdhRadio = 0.0;
  private double globalHeightRadio = 0.0;

  public MatchState(Section patternSectionNow) {
    this.patternSectionNow = patternSectionNow;
    updatePatternBounds(patternSectionNow);
  }

  public Section getPatternSectionNow() {
    return patternSectionNow;
  }

  public void setPatternSectionNow(Section patternSectionNow) {
    this.patternSectionNow = patternSectionNow;
  }

  public Boolean isFinish() {
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
  public Boolean checkSign(Section section) {
    if (section.getSign() == patternSectionNow.getSign()) {
      if (startSectionId == -1) {
        startSectionId = section.getId();
      }
      updateDataBounds(section);
      return true;
    }

    shapeError++;
    if (shapeError <= shapeTolerance) {
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
    TotalUpHeight += section.getUpHeight();

    if (section.getMaxHeight() + TotalUpHeight > patternMaxHeight) {
      patternMaxHeight = section.getMaxHeight() + TotalUpHeight;
    }
    if (section.getMinHeight() + TotalUpHeight < patternMinHeight) {
      patternMinHeight = section.getMinHeight() + TotalUpHeight;
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

  public void calcGlobalRadio() {
    globalHeightRadio = (dataMaxHeight - dataMinHeight) / (patternMaxHeight - patternMinHeight);
    globalWitdhRadio = (dataMaxWidth - dataMinWidth) / (patternMaxWidth - patternMinWidth);
  }

  public Boolean calcOneSectionMatchValue(Section section, double smoothValue, double threshold) {
    // calc the LED
    // this is the Rx and Ry in the paper
    double localWidthRadio =
        section.getWidthBound() / (patternSectionNow.getWidthBound() * globalWitdhRadio);

    // smooth value is used to avoid the zero division
    double localHeightUp = Math.max(section.getHeightBound(), smoothValue);
    double localHeightDown =
        Math.max(patternSectionNow.getHeightBound() * globalHeightRadio, smoothValue);
    double localHeightRadio = localHeightUp / localHeightDown;

    double LED = Math.pow(Math.log(localWidthRadio), 2) + Math.pow(Math.log(localHeightRadio), 2);

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

    double numRadio = ((double) dataPointNum) / ((double) patternPointNum);

    double shapeError = 0.0;

    for (int i = 1; i < dataPointNum; i++) {
      double pointHeight = getPointHeight(i, numRadio, patternPointNum);

      shapeError +=
          pointHeight * globalHeightRadio * localHeightRadio
              - alignHeightDiff
              - section.getPoints().get(i).y;
    }

    shapeError = shapeError / ((dataMaxHeight - dataMinHeight) * section.getPoints().size());

    // calc the match value for a section
    matchValue = LED + shapeError;

    dataSectionList.add(section);

    if (patternSectionNow.isFinal()) {
      isFinish = true;
    }

    if (isFinish || matchValue > threshold || (patternSectionNow.getNextSectionList() == null)) {
      return true;
    } else {
      patternSectionNow = patternSectionNow.getNextSectionList().get(0);
      return false;
    }
  }

  private double getPointHeight(int i, double numRadio, int patternPointNum) {
    double patternIndex = i * numRadio;
    int leftIndex = (int) patternIndex;
    double leftRadio = patternIndex - leftIndex;
    int rightIndex = leftIndex >= patternPointNum ? patternPointNum : leftIndex + 1;
    double rightRadio = 1 - leftRadio;
    // the heightValue is a weighted avg about the index near num
    return patternSectionNow.getPoints().get(leftIndex).y * rightRadio
        + patternSectionNow.getPoints().get(rightIndex).y * leftRadio;
  }
}
