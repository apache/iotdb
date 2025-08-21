package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch;

import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.MatchState;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.PatternSegment;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.Point;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.RegexMatchState;
import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.Section;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.gapTolerance;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.lineSectionTolerance;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.PatternSegment.tangent;

public class QetchAlgorthm {
  private Integer pointNum = -1;

  private Boolean isNewDataSegment = true;

  private Boolean isRegex = false;
  private Section stateMachineStartSection = null;

  private Double smoothValue = (double) 0;
  private Double threshold = (double) 0;
  private Double widthLimit = (double) 0;
  private Double heightLimit = (double) 0;

  private Boolean isPatternFromOrigin = false;

  private Point lastPoint = null;

  private Section dataLastLastSection = null;
  private Section dataLastSection = null;
  private Section nowSection = null;

  private Queue<Section> dataSectionQueue = new LinkedList<>();
  private Queue<MatchState> stateQueue = new LinkedList<>();
  private Queue<RegexMatchState> regexStateQueue = new LinkedList<>();
  private RegexMatchState regexMatchState = null;

  private int matchResultID = 0;

  private Double gap = (double) 0;
  int dataSectionIndex = 0;

  private MatchState matchResult = null; // each one in it is a point to the start
  private RegexMatchState regexMatchResult =
      null; // each one in it is a list of matchResult which has the same start section

  private Queue<Section> sectionListForDelay = new LinkedList<>(); // only use for delay match

  public QetchAlgorthm() {
    pointNum = -1;
    isNewDataSegment = true;
  }

  private List<PatternSegment> parsePattern2DataSegment(String pattern) {
    // this pattern is divided by ",", such as "{,1,2,1,},3,6,9"
    // "()" claim as a point while "{}" claim as a repeat regex sign "+" which is supported to nest

    List<String> patternPiecesTemp = Arrays.asList(pattern.split(","));
    List<String> patternPieces = new ArrayList<>();
    for (int i = 0; i < patternPiecesTemp.size(); i++) {
      String piece = patternPiecesTemp.get(i);
      // scan the piece and divide the {. }*, }+ out of it, and push them to the list in order
      for (int j = 0; j < piece.length(); j++) {
        char c = piece.charAt(j);
        if (c == '{') {
          patternPieces.add("{");
        } else if (c == '}') {
          if (j + 1 < piece.length() && piece.charAt(j + 1) == '*') {
            patternPieces.add("}*");
            j++;
          } else if (j + 1 < piece.length() && piece.charAt(j + 1) == '+') {
            patternPieces.add("}+");
            j++;
          } else {
            throw new IllegalArgumentException(
                "Invalid pattern: " + pattern + ", missing repeat sign after '}'");
          }
        } else if (Character.isDigit(c) || c == '.' || c == '-') {
          // scan the number, and push it to the list
          StringBuilder numBuilder = new StringBuilder();
          while (j < piece.length()
              && (Character.isDigit(piece.charAt(j))
                  || piece.charAt(j) == '.'
                  || piece.charAt(j) == '-')) {
            numBuilder.append(piece.charAt(j));
            j++;
          }
          j--; // because the for loop will increase j, so need to decrease it
          patternPieces.add(numBuilder.toString());
        }
      }
    }

    // classify the Pieces to different dataSegment
    List<PatternSegment> patternSegments = new ArrayList<>();
    PatternSegment patternSegment = null;

    int numIndex = -1;
    int pointIndex = -1;
    for (int i = 0; i < patternPieces.size(); i++) {
      String piece = patternPieces.get(i);
      if (piece.equals("{") || piece.equals("}*") || piece.equals("}+")) {
        isRegex = true;
        if (patternSegment != null) {
          patternSegments.add(patternSegment);
          numIndex = patternSegments.size() - 1;
        }
        patternSegment = null;
        patternSegments.add(new PatternSegment(piece));
      } else {
        // tips: every dataSegment should be ended with a point which is the start of the next
        // dataSegment
        pointIndex++;
        Double num = Double.parseDouble(piece);
        Point point = new Point(pointIndex, num);
        if (patternSegment == null) {
          if (numIndex >= 0) {
            patternSegments.get(numIndex).addPoint(new Point(pointIndex, num));
          }
          patternSegment = new PatternSegment();
        }
        patternSegment.addPoint(point);
      }
    }
    if (patternSegment != null && patternSegment.getPoints().size() >= 2)
      patternSegments.add(patternSegment);

    return patternSegments;
  }

  private void transSectionListToAutomatonInRegex(List<Section> startSections) {
    // index the section in the automaton
    int id = 0;
    Queue<Section> queue = new LinkedList<>();
    if (startSections.size() >= 2) {
      // set a virtual section as a new startSection
      Section virtualStartSection = new Section(2); // use 2 to claim the start section is virtual
      for (Section section : startSections) {
        virtualStartSection.getNextSectionList().add(section);
      }
      queue.add(virtualStartSection);
      stateMachineStartSection = virtualStartSection;
    } else {
      queue.add(startSections.get(0));
      stateMachineStartSection = startSections.get(0);
    }

    while (!queue.isEmpty()) {
      Section section = queue.poll();
      if (section.isVisited()) {
        continue;
      }
      section.setId(id++);
      section.setIsVisited(true);
      // push all the next section to the queue
      for (Section nextSection : section.getNextSectionList()) {
        if (!nextSection.isVisited()) {
          queue.add(nextSection);
        }
      }
    }
  }

  private void transDataSegment2Automation(List<PatternSegment> patternSegments) {
    // loop each dataSegment and trans them to sectionList
    int lastSegmentIndex = 0;
    for (int i = 0; i < patternSegments.size(); i++) {
      if (!patternSegments.get(i).isConstantChar()) {
        patternSegments.get(i).trans2SectionList(isPatternFromOrigin, smoothValue);
        if (i > lastSegmentIndex) {
          lastSegmentIndex = i;
        }
      }
    }

    // need to tag which is the start section and which is the end section
    if (isRegex) {
      // use a stack to concat all dataSegment to one automaton
      Stack<PatternSegment> stack = new Stack<>();
      for (int i = 0; i < patternSegments.size(); i++) {
        if (patternSegments.get(i).isConstantChar()
            && (patternSegments.get(i).getRepeatSign().equals("}*")
                || patternSegments.get(i).getRepeatSign().equals("}+"))) {
          // pop the dataSegment from stack until find the '{'
          PatternSegment patternSegment = stack.pop();
          while (!stack.isEmpty() && !stack.peek().isConstantChar()) {
            PatternSegment topSegment = stack.pop();
            patternSegment.concatNear(topSegment);
          }
          stack.pop(); // pop the '{'
          patternSegment.concatHeadAndTail();
          if (patternSegments.get(i).getRepeatSign().equals("}*")) {
            patternSegment.setIsZeroRepeat(true);
          }
          stack.push(patternSegment);
        } else {
          // push the dataSegment to stack
          stack.push(patternSegments.get(i));
        }
      }

      // loop the stack and concat all dataSegment to one automaton
      PatternSegment patternSegment = stack.pop();
      while (!stack.isEmpty()) {
        PatternSegment topSegment = stack.pop();
        patternSegment.concatNear(topSegment);
      }
      for (Section section : patternSegment.getEndSectionList()) {
        section.setIsFinal(true);
      }

      transSectionListToAutomatonInRegex(patternSegment.getStartSectionList());
    } else {
      // only one dataSegment, no need to concat
      // trans to the automaton
      patternSegments.get(0).getEndSectionList().get(0).setIsFinal(true);
      stateMachineStartSection = patternSegments.get(0).getStartSectionList().get(0);
    }
  }

  public void parsePattern2Automaton(String pattern) {
    // divided the pattern to multi DataSegment with the regex information
    // TODO need to check the validity of the pattern, such as the repeat start and end need to be
    // matched
    List<PatternSegment> patternSegments = parsePattern2DataSegment(pattern);

    // trans multi dataSegment to automaton
    transDataSegment2Automation(patternSegments);
  }

  // only use after the result has been printed out
  public void environmentClear() {
    // only keep the lastPoint, else set null
    this.dataLastLastSection = null;
    this.dataLastSection = null;
    this.nowSection = null;

    this.dataSectionQueue.clear();
    this.stateQueue.clear();
    this.regexStateQueue.clear();
    this.regexMatchState = null;

    this.gap = (double) 0;
    this.dataSectionIndex = 0;
  }

  public void matchResultClear() {
    this.matchResult = null;
    this.regexMatchResult = null;
  }

  public Boolean hasMatchResult() {
    return this.matchResult != null;
  }

  // use while the dataSegment is finished, and want to print out the result
  public MatchState getMatchResult() {
    return this.matchResult;
  }

  public Boolean hasRegexMatchResult() {
    return this.isRegex && this.regexMatchResult != null;
  }

  public RegexMatchState getRegexMatchResult() {
    return this.regexMatchResult;
  }

  // use in constant automaton
  private void calcMatchValue(MatchState matchState) {
    // in constant the less the id is , the quicker the match, so the beginning of the queue is the
    // beginning of the matchPath

    // scan from the start to the end, calc the match value, if it is more than threshold, cut this
    // branch, if it is less than threshold, add it to the result
    // One result only record the calc result distance, the start section, the length of the
    // resultSectionList
    matchState.setPatternSectionNow(stateMachineStartSection);
    matchState.calcGlobalRadio(smoothValue);
    while (!dataSectionQueue.isEmpty()) {
      // get the first state
      Section section = dataSectionQueue.poll();
      if (matchState.calcOneSectionMatchValue(section, smoothValue, threshold)) {
        break;
      }
    }
    if (matchState.isFinish()) {
      matchResult = matchState;
    }
  }

  // check the bound limit
  private Boolean checkBoundLimit(MatchState state) {
    return (state.getDataHeightBound() <= this.heightLimit)
        && (state.getDataWidthBound() <= this.widthLimit);
  }

  // use in constant automaton
  private void transition(Section section) {
    Queue<MatchState> tempStateQueue = new LinkedList<>();
    while (!stateQueue.isEmpty()) {
      // get the first state
      MatchState state = stateQueue.poll();
      // while get true , add it to the queue
      if (state.checkSign(section)) {
        if (checkBoundLimit(state)) {
          if (state.getPatternSectionNow().isFinal()) {
            calcMatchValue(state);
            continue;
          }
          state.nextPatternSection();
          tempStateQueue.add(state);
        }
      }
    }
    stateQueue = tempStateQueue;
  }

  private void addSectionInConstant(Section section) {
    section.setId(dataSectionIndex++);
    dataSectionQueue.add(section);

    // scan all start in one loop, and no record the calc result because the pair only use once and
    // no need to record
    stateQueue.add(new MatchState(stateMachineStartSection));
    transition(section);

    double lastToThrowIndex = 0;
    if (stateQueue.isEmpty()) {
      lastToThrowIndex = dataSectionIndex;
    } else {
      lastToThrowIndex = stateQueue.peek().getStartSectionId();
    }

    // remove the section which is no need to release memory
    while (!dataSectionQueue.isEmpty() && dataSectionQueue.peek().getId() < lastToThrowIndex) {
      dataSectionQueue.poll();
    }
  }

  private void addSectionInRegex(Section section) {
    section.setId(dataSectionIndex++);
    dataSectionQueue.add(section);

    if (regexMatchState == null) {
      regexMatchState = new RegexMatchState(stateMachineStartSection);
    }

    if (regexMatchState.addSection(
        section,
        heightLimit,
        widthLimit,
        smoothValue,
        threshold)) { // claim that regexMatchState match is over
      dataSectionQueue.poll();
      if (!regexMatchState.getMatchResult().isEmpty()) {
        // TODO clean the variable which no need
        regexMatchResult = regexMatchState;
        regexMatchState = null;
        return;
      }
      boolean isNext = true;
      regexMatchState = null;
      while (!dataSectionQueue.isEmpty() && isNext) {
        isNext = false;
        regexMatchState = new RegexMatchState(stateMachineStartSection);
        Iterator<Section> iterator = dataSectionQueue.iterator();
        while (iterator.hasNext()) {
          Section dataSection = iterator.next();
          if (regexMatchState.addSection(
              dataSection, heightLimit, widthLimit, smoothValue, threshold)) {
            dataSectionQueue.poll();
            if (!regexMatchState.getMatchResult().isEmpty()) {
              regexMatchResult = regexMatchState;
              regexMatchState = null;
              return;
            }
            isNext = true;
            break;
          }
        }
      }
    }
  }

  public Boolean checkNextMatchResult() {
    while (!sectionListForDelay.isEmpty()) {
      Section section = sectionListForDelay.poll();
      addSectionInConstant(section);
      if (hasMatchResult()) {
        return true;
      }
    }
    return false;
  }

  public Boolean checkNextRegexMatchResult() {
    boolean isNext = true;
    while (!dataSectionQueue.isEmpty() && isNext) {
      isNext = false;
      regexMatchState = new RegexMatchState(stateMachineStartSection);
      Iterator<Section> iterator = dataSectionQueue.iterator();
      while (iterator.hasNext()) {
        Section dataSection = iterator.next();
        if (regexMatchState.addSection(
            dataSection, heightLimit, widthLimit, smoothValue, threshold)) {
          dataSectionQueue.poll();
          if (!regexMatchState.getMatchResult().isEmpty()) {
            regexMatchResult = regexMatchState;
            regexMatchState = null;
            return true;
          }
          regexMatchState = null;
          isNext = true;
          break;
        }
      }
    }
    regexMatchState = new RegexMatchState(stateMachineStartSection);
    while (!sectionListForDelay.isEmpty()) {
      Section section = sectionListForDelay.poll();
      addSectionInRegex(section);
      if (!regexMatchState.getMatchResult().isEmpty()) {
        return true;
      }
    }
    return false;
  }

  private void addSection(Section section) {
    if (hasMatchResult() || hasRegexMatchResult()) {
      sectionListForDelay.add(section);
    } else {
      if (isRegex) {
        addSectionInRegex(section);
      } else {
        addSectionInConstant(section);
      }
    }
  }

  private void closeNowSection() {
    if (nowSection.getHeightBound() <= smoothValue) {
      nowSection.setSign(0);
    }

    if (dataLastSection == null) {
      dataLastSection = nowSection;
    } else {
      // the dataLastSection is no null, so need to check whether to concat
      if (dataLastSection.getSign() != 0) {
        if (dataLastLastSection != null) {
          addSection(dataLastLastSection);
        }
        dataLastLastSection = dataLastSection;
        dataLastSection = nowSection;
      } else {
        // the dataLastSection is sign 0

        // while the sign of last section is 0, now it need to check two case which one is now
        // section sign is 0, other is not
        if (nowSection.getSign() == 0) {
          // 1 case: there are two section sign is 0, which need to concat them
          List<Section> concatResult = dataLastSection.concat(nowSection, smoothValue);
          // the result size only can be 1 or 3
          // in this case, also divided to two subcase, one is the concatResult is higher than
          // smoothValue, the other is lower than smoothValue
          // 1.1 case: the concat result is higher than smoothValue, the concatResult need to
          // divided to three section
          // A B C D and check B is long enough to be a line section or combine the ABC or AB(move
          // index to C D, remind to add connect between the section) which up to whether the sign
          // of A and C is same
          if (concatResult.size() == 3) {
            if (dataLastLastSection == null) {
              addSection(concatResult.get(0));
              dataLastLastSection = concatResult.get(1);
              dataLastSection = concatResult.get(2);
            } else {
              // check B is long enought to be a line section
              if (concatResult.get(0).getPoints().size() <= lineSectionTolerance) {
                // check whether the sign of A and C is same ( because the B is sign 0, so A and C
                // sign isn't 0)
                if (dataLastLastSection.getSign() == concatResult.get(1).getSign()) {
                  // combine the ABC
                  dataLastLastSection.combine(concatResult.get(0));
                  dataLastLastSection.combine(concatResult.get(1));
                  dataLastSection = concatResult.get(2);
                } else {
                  // combine the AB
                  dataLastLastSection.combine(concatResult.get(0));
                  addSection(dataLastLastSection);
                  dataLastLastSection = concatResult.get(1);
                  dataLastSection = concatResult.get(2);
                }
              } else {
                // A,B sent to calc and move to C D
                addSection(dataLastLastSection);
                addSection(concatResult.get(0));
                dataLastLastSection = concatResult.get(1);
                dataLastSection = concatResult.get(2);
              }
            }
          } else if (concatResult.size() == 2) {
            // 1.2 case: the concat result is lower than smoothValue, no need to divided
            // A B and check B is long enough to be a line section or combine the AB(move index to C
            // D) which up to whether the sign of A and C is same
            if (dataLastLastSection == null) {
              dataLastLastSection = concatResult.get(0);
              dataLastSection = concatResult.get(1);
            } else {
              if (concatResult.get(0).getSign() == 0) {
                if (concatResult.get(0).getPoints().size() <= lineSectionTolerance) {
                  // check whether the sign of A and C is same ( because the B is sign 0, so A and C
                  // sign isn't 0)
                  if (dataLastLastSection.getSign() == concatResult.get(1).getSign()) {
                    // combine the ABC
                    dataLastLastSection.combine(concatResult.get(0));
                    dataLastLastSection.combine(concatResult.get(1));
                    dataLastSection = null;
                  } else {
                    // AB and C
                    dataLastLastSection.combine(concatResult.get(0));
                    dataLastSection = concatResult.get(1);
                  }
                } else {
                  // A sent to calc and move to B C
                  addSection(dataLastLastSection);
                  dataLastLastSection = concatResult.get(0);
                  dataLastSection = concatResult.get(1);
                }
              } else {
                if (dataLastLastSection.getSign() == concatResult.get(0).getSign()) {
                  // combine the AB
                  dataLastLastSection.combine(concatResult.get(0));
                  dataLastSection = concatResult.get(1);
                } else {
                  // A is sent to calc and move to B C
                  addSection(dataLastLastSection);
                  dataLastLastSection = concatResult.get(0);
                  dataLastSection = concatResult.get(1);
                }
              }
            }
          }
          // 1.2 case: the concat result is lower than smoothValue, no need to divided
          else {
            dataLastSection = concatResult.get(0);
          }
        } else {
          if (dataLastLastSection == null) {
            dataLastLastSection = dataLastSection;
            dataLastSection = nowSection;
          } else {
            // 2 case: A B C and check B is long enough to be a line section or combine the ABC or
            // AB(move index to AB C) which up to whether the sign of A and C is same
            if (dataLastSection.getPoints().size() <= lineSectionTolerance) {
              if (dataLastLastSection.getSign() == nowSection.getSign()) {
                // combine the ABC
                dataLastLastSection.combine(dataLastSection);
                dataLastLastSection.combine(nowSection);
                dataLastSection = null;
              } else {
                // combine the AB
                dataLastLastSection.combine(dataLastSection);
                dataLastSection = nowSection;
              }
            } else {
              // A is sent to calc and move to B C
              addSection(dataLastLastSection);
              dataLastLastSection = dataLastSection;
              dataLastSection = nowSection;
            }
          }
        }
      }
    }
  }

  public void closeNowDataSegment() {
    // close之后，last和lastlast至少有一个有数据
    closeNowSection();
    if (dataLastLastSection != null && dataLastSection != null) {
      if (dataLastSection.getSign() == 0
          && dataLastSection.getPoints().size() <= lineSectionTolerance) {
        dataLastLastSection.combine(dataLastSection);
        addSection(dataLastLastSection);
      } else {
        addSection(dataLastLastSection);
        dataLastSection.setIsFinal(true);
        addSection(dataLastSection);
      }
    } else {
      // 这里看起来是前后判断，实际上这两个是相互独立的，不会同时不为null
      if (dataLastLastSection != null) {
        dataLastLastSection.setIsFinal(true);
        addSection(dataLastLastSection);
      }
      if (dataLastSection != null) {
        dataLastSection.setIsFinal(true);
        addSection(dataLastSection);
      }
    }
    isNewDataSegment = true;
  }

  public void addPoint(Point point) {
    if (isNewDataSegment) {
      environmentClear();
      isNewDataSegment = false;
    }
    if (lastPoint == null) {
      lastPoint = point;
    } else {
      if (this.gap <= 0) {
        this.gap = Math.abs(point.x - lastPoint.x);
      } else {
        // input data is considered as a same time gap data, so the big gap only happen in the
        // different dataSegment
        if (Math.abs(point.x - lastPoint.x) > gapTolerance * this.gap) {
          lastPoint = point;
          closeNowDataSegment();
          return;
        }
      }

      double tangent = tangent(lastPoint, point);
      double sign = Math.signum(tangent);
      if (nowSection == null) {
        nowSection = new Section(sign);
        nowSection.addPoint(lastPoint);
      } else {
        if (sign != nowSection.getSign()) {
          closeNowSection();
          nowSection = null;
          nowSection = new Section(sign);
          nowSection.addPoint(lastPoint);
        }
      }
      nowSection.addPoint(point);
      lastPoint = point;
    }
  }

  // variables getter and setter
  public void setThreshold(Double threshold) {
    this.threshold = threshold;
  }

  public void setSmoothValue(Double smoothValue) {
    this.smoothValue = smoothValue;
  }

  public void setWidthLimit(Double widthLimit) {
    this.widthLimit = widthLimit;
  }

  public void setHeightLimit(Double heightLimit) {
    this.heightLimit = heightLimit;
  }

  public void setIsPatternFromOrigin(Boolean isPatternFromOrigin) {
    this.isPatternFromOrigin = isPatternFromOrigin;
  }

  public Boolean isRegex() {
    return isRegex;
  }

  public Integer getPointNum() {
    pointNum++;
    return pointNum;
  }

  public int getMatchResultID() {
    return matchResultID++;
  }
}
