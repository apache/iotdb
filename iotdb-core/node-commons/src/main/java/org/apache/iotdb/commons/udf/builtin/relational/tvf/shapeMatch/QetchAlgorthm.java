package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch;

import org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.*;

import java.util.*;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.*;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model.PatternSegment.tangent;

public class QetchAlgorthm {
    private Boolean isRegex = false;
    private Automaton automaton = new Automaton();

    private Double smoothValue = (double) 0;
    private Double threshold = (double) 0;
    private Double widthLimit = (double) 0;
    private Double heightLimit = (double) 0;

    private String Type = "shape";

    private Point lastPoint = null;

    private Section dataLastLastSection = null;
    private Section dataLastSection = null;
    private Section nowSection = null;

    private Queue<Section> dataSectionQueue = new LinkedList<>();
    private Queue<MatchState> stateQueue = new LinkedList<>();
    private Queue<RegexMatchState> regexStateQueue = new LinkedList<>();
    private RegexMatchState regexMatchState = null;

    private Double gap = (double) 0;
    int dataSectionIndex = 0;

    private List<MatchState> matchResult = new ArrayList<>(); // each one in it is a point to the start
    private List<RegexMatchState> regexMatchResult = new ArrayList<>(); // each one in it is a list of matchResult which has the same start section

    public QetchAlgorthm(){}

    private List<PatternSegment> parsePattern2DataSegment(String pattern){
        // this pattern is divided by ",", such as "{.(1,1).(2,2).(3,1).}.(4,3).(5,6).(6,9)"
        // "()" claim as a point while "{}" claim as a repeat regex sign "+" which is supported to nest
        List<String> patternPieces = Arrays.asList(pattern.split("\\."));

        // prepare the minY and minX
        Double minX = Double.MAX_VALUE;
        Double minY = Double.MAX_VALUE;

        // classify the Pieces to different dataSegment
        List<PatternSegment> patternSegments = new ArrayList<>();
        PatternSegment patternSegment = null;

        for(int i = 0; i < patternPieces.size(); i++){
            String piece = patternPieces.get(i);
            if(piece.equals("{") || piece.equals("}")){
                isRegex = true;
                if(patternSegment != null) patternSegments.add(patternSegment);
                patternSegment = null;
                patternSegments.add(new PatternSegment(piece.charAt(0)));
            }
            else{
                // tips: every dataSegment should be ended with a point which is the start of the next dataSegment
                Point point = new Point(piece);
                if(patternSegment == null){
                    patternSegments.get(patternSegments.size() - 1).addPoint(point);
                    patternSegment = new PatternSegment();
                }
                patternSegment.addPoint(point);
                if(minX > point.x){
                    minX = point.x;
                }
                if(minY > point.y){
                    minY = point.y;
                }
            }
        }
        if(patternSegment != null) patternSegments.add(patternSegment);

        // move the minX and minY to the (0,0)
        for (PatternSegment segment : patternSegments) {
            List<Point> points = segment.getPoints();
            for (Point point : points) {
                System.out.println("x: " + point.x + " y: " + point.y);
                point.setXY(point.x - minX, point.y - minY);
                System.out.println("x: " + point.x + " y: " + point.y);
            }
        }

        // set the last section in the last dataSegment as the end section
        PatternSegment lastSegment = patternSegments.get(patternSegments.size() - 1);
        Section lastSection = lastSegment.getSections().get(lastSegment.getSections().size() - 1);
        lastSection.setIsFinal(true);

        return patternSegments;
    }

    private void transSectionListToAutomatonInRegex(Section startSection){
        // index the section in the automaton
        int id = 0;
        Queue<Section> queue = new LinkedList<>();
        queue.add(startSection);
        while(!queue.isEmpty()){
            Section section = queue.poll();
            if(!section.isVisited() || (!section.isFinal() && section.getNextSectionList().isEmpty())){
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

        this.automaton.setStartSection(startSection);
        this.automaton.setStateNum(id);
    }

    private void transDataSegment2Automation(List<PatternSegment> patternSegments){
        // loop each dataSegment and trans them to sectionList
        for(int i = 0; i < patternSegments.size() ; i++){
            Type = isRegex? "shape": Type;
            patternSegments.get(i).trans2SectionList(Type, smoothValue);
        }

        // need to tag which is the start section and which is the end section
        if(isRegex){
            // use a stack to concat all dataSegment to one automaton
            Stack<PatternSegment> stack = new Stack<>();
            for (int i = 0; i < patternSegments.size(); i++) {
                if(patternSegments.get(i).isConstantChar() && patternSegments.get(i).getConstantChar() == '}'){
                    // pop the dataSegment from stack until find the '{'
                    PatternSegment patternSegment = stack.pop();
                    while (!stack.isEmpty() && !stack.peek().isConstantChar()) {
                        PatternSegment topSegment = stack.pop();
                        patternSegment.concatNear(topSegment);
                    }
                    stack.pop(); // pop the '{'
                    patternSegment.concatHeadAndTail();
                    stack.push(patternSegment);
                }
                else {
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

            transSectionListToAutomatonInRegex(patternSegment.getSections().get(0));
        }
        else{
            // only one dataSegment, no need to concat
            // trans to the automaton
            this.automaton.setStartSection(patternSegments.get(0).getSections().get(0));
            this.automaton.setStateNum(patternSegments.get(0).getSections().size());
        }
    }

    public void parsePattern2Automaton(String pattern){
        // divided the pattern to multi DataSegment with the regex information
        // TODO need to check the validity of the pattern, such as the repeat start and end need to be matched
        List<PatternSegment> patternSegments = parsePattern2DataSegment(pattern);

        // trans multi dataSegment to automaton
        transDataSegment2Automation(patternSegments);
    }

    // only use after the result has been printed out
    public void environmentClear(){
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

        this.matchResult.clear();
        this.regexMatchResult.clear();
    }

    // use while the dataSegment is finished, and want to print out the result
    public List<MatchState> getMatchResult(){
        return this.matchResult;
    }

    // use in constant automaton
    private void calcMatchValue(MatchState matchState){
        // in constant the less the id is , the quicker the match, so the beginning of the queue is the beginning of the matchPath

        // scan from the start to the end, calc the match value, if it is more than threshold, cut this branch, if it is less than threshold, add it to the result
        // One result only record the calc result distance, the start section, the length of the resultSectionList
        matchState.setPatternSectionNow(automaton.getStartSection());
        matchState.calcGlobalRadio();
        while(true){
            // get the first state
            Section section = dataSectionQueue.poll();
            if(matchState.calcOneSectionMatchValue(section, smoothValue, threshold)){
                break;
            }
        }
        if(matchState.isFinish()){
            // TODO clean the variable which is no need
            matchResult.add(matchState);
        }
    }

    // check the bound limit
    private Boolean checkBoundLimit(MatchState state){
        return (state.getDataHeightBound() <= this.heightLimit) && (state.getDataWidthBound() <= this.widthLimit);
    }

    // use in constant automaton
    private void transition(Section section) {
        Queue<MatchState> tempStateQueue = new LinkedList<>();
        while(!stateQueue.isEmpty()){
            // get the first state
            MatchState state = stateQueue.poll();
            // while get true , add it to the queue
            if(state.checkSign(section)){
                if(checkBoundLimit(state)){
                    if(state.getPatternSectionNow().isFinal()){
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

    private void addSectionInConstant(Section section){
        section.setId(dataSectionIndex++);
        dataSectionQueue.add(section);

        // scan all start in one loop, and no record the calc result because the pair only use once and no need to record
        stateQueue.add(new MatchState(this.automaton.getStartSection()));
        transition(section);

        double lastToThrowIndex = 0;
        if(stateQueue.isEmpty()){
            lastToThrowIndex = dataSectionIndex - 1;
        }
        else{
            lastToThrowIndex = stateQueue.peek().getStartSectionId();
        }

        // remove the section which is no need to release memory
        while(!dataSectionQueue.isEmpty() && dataSectionQueue.peek().getId() <= lastToThrowIndex){
            dataSectionQueue.poll();
        }
    }

    private void addSectionInRegex(Section section){
        section.setId(dataSectionIndex++);
        dataSectionQueue.add(section);

        if(regexMatchState == null){
            regexMatchState = new RegexMatchState(this.automaton.getStartSection());
        }

        boolean isNext = true;
        if(regexMatchState.addSection(section,heightLimit,widthLimit, smoothValue, threshold)) { // claim that regexMatchState match is over
            if (!regexMatchState.getMatchResult().isEmpty()){
                // TODO clean the variable which no need
                regexMatchResult.add(regexMatchState);
            }
            dataSectionQueue.poll();
            while (!dataSectionQueue.isEmpty() && isNext){
                isNext = false;
                regexMatchState = new RegexMatchState(this.automaton.getStartSection());
                Iterator<Section> iterator = dataSectionQueue.iterator();
                while (iterator.hasNext()){
                    Section dataSection = iterator.next();
                    if(regexMatchState.addSection(dataSection,heightLimit,widthLimit, smoothValue, threshold)){
                        if (!regexMatchState.getMatchResult().isEmpty()){
                            // TODO clean the variable which no need
                            regexMatchResult.add(regexMatchState);
                        }
                        dataSectionQueue.poll();
                        isNext = true;
                        break;
                    }
                }
            }
        }
    }

    private void addSection(Section section){
        if(isRegex){
            addSectionInRegex(section);
        }
        else{
            addSectionInConstant(section);
        }
    }

    private void closeNowSection(){
        if(nowSection.getHeightBound() <= smoothValue){
            nowSection.setSign(0);
        }

        if(dataLastSection == null){
            dataLastSection = nowSection;
        }
        else{
            // the dataLastSection is no null, so need to check whether to concat
            if(dataLastSection.getSign() != 0){
                if(dataLastLastSection != null){
                    addSection(dataLastLastSection);
                }
                dataLastLastSection = dataLastSection;
                dataLastSection = nowSection;
            }
            else{
                // the dataLastSection is sign 0

                // while the sign of last section is 0, now it need to check two case which one is now section sign is 0, other is not
                if(nowSection.getSign() == 0){
                    // 1 case: there are two section sign is 0, which need to concat them
                    List<Section> concatResult = dataLastSection.concat(nowSection, smoothValue);
                    // the result size only can be 1 or 3
                    // in this case, also divided to two subcase, one is the concatResult is higher than smoothValue, the other is lower than smoothValue
                    // 1.1 case: the concat result is higher than smoothValue, the concatResult need to divided to three section
                    // A B C D and check B is long enough to be a line section or combine the ABC or AB(move index to C D, remind to add connect between the section) which up to whether the sign of A and C is same
                    if(concatResult.size() == 3){
                        if(dataLastLastSection == null){
                            addSection(concatResult.get(0));
                            dataLastLastSection = concatResult.get(1);
                            dataLastSection = concatResult.get(2);
                        }
                        else{
                            // check B is long enought to be a line section
                            if(concatResult.get(0).getPoints().size() <= lineSectionTolerance) {
                                // check whether the sign of A and C is same ( because the B is sign 0, so A and C sign isn't 0)
                                if(dataLastLastSection.getSign() == concatResult.get(1).getSign()) {
                                    // combine the ABC
                                    dataLastLastSection.combine(concatResult.get(0));
                                    dataLastLastSection.combine(concatResult.get(1));
                                    dataLastSection = concatResult.get(2);
                                }
                                else {
                                    // combine the AB
                                    dataLastLastSection.combine(concatResult.get(0));
                                    addSection(dataLastLastSection);
                                    dataLastLastSection = concatResult.get(1);
                                    dataLastSection = concatResult.get(2);
                                }
                            }
                            else{
                                // A,B sent to calc and move to C D
                                addSection(dataLastLastSection);
                                addSection(concatResult.get(0));
                                dataLastLastSection = concatResult.get(1);
                                dataLastSection = concatResult.get(2);
                            }
                        }

                    }
                    else if (concatResult.size() == 2){
                        // 1.2 case: the concat result is lower than smoothValue, no need to divided
                        // A B and check B is long enough to be a line section or combine the AB(move index to C D) which up to whether the sign of A and C is same
                        if(dataLastLastSection == null){
                            dataLastLastSection = concatResult.get(0);
                            dataLastSection = concatResult.get(1);
                        }
                        else{
                            if(concatResult.get(0).getSign() == 0){
                                // check whether the sign of A and C is same ( because the B is sign 0, so A and C sign isn't 0)
                                if(dataLastLastSection.getSign() == concatResult.get(1).getSign()){
                                    // combine the ABC
                                    dataLastLastSection.combine(concatResult.get(0));
                                    dataLastLastSection.combine(concatResult.get(1));
                                    dataLastSection = null;
                                }
                                else{
                                    // AB and C
                                    dataLastLastSection.combine(concatResult.get(0));
                                    dataLastSection = concatResult.get(1);
                                }
                            }
                            else {
                                if(dataLastLastSection.getSign() == concatResult.get(0).getSign()){
                                    // combine the AB
                                    dataLastLastSection.combine(concatResult.get(0));
                                    dataLastSection = concatResult.get(1);
                                }
                                else{
                                    // A is sent to calc and move to B C
                                    addSection(dataLastLastSection);
                                    dataLastLastSection = concatResult.get(0);
                                    dataLastSection = concatResult.get(1);
                                }
                            }
                        }
                    }
                    // 1.2 case: the concat result is lower than smoothValue, no need to divided
                    else{
                        dataLastSection = concatResult.get(0);
                    }
                }
                else{
                    if(dataLastLastSection == null){
                        dataLastLastSection = dataLastSection;
                        dataLastSection = nowSection;
                    }
                    else{
                        // 2 case: A B C and check B is long enough to be a line section or combine the ABC or AB(move index to AB C) which up to whether the sign of A and C is same
                        if(dataLastSection.getPoints().size() <= lineSectionTolerance){
                            if(dataLastLastSection.getSign() == nowSection.getSign()){
                                // combine the ABC
                                dataLastLastSection.combine(dataLastSection);
                                dataLastLastSection.combine(nowSection);
                                dataLastSection = null;
                            }
                            else{
                                // combine the AB
                                dataLastLastSection.combine(dataLastSection);
                                dataLastSection = nowSection;
                            }
                        }
                        else{
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

    public void closeNowDataSegment(){
        closeNowSection();
        if(dataLastLastSection != null){
            addSection(dataLastLastSection);
        }
        if(dataLastSection != null){
            addSection(dataLastSection);
        }
    }
    
    public Boolean addPoint(Point point){
        if(lastPoint == null){
            lastPoint = point;
        }
        else{
            if(this.gap <= 0){
                this.gap = Math.abs(point.x - lastPoint.x);
            }
            else{
                // input data is considered as a same time gap data, so the big gap only happen in the different dataSegment
                if(Math.abs(point.x - lastPoint.x) > gapTolerance*this.gap){
                    lastPoint = point;
                    closeNowDataSegment();
                    return true;
                }
            }

            double tangent = tangent(lastPoint, point);
            double sign = Math.signum(tangent);
            if(nowSection == null){
                nowSection = new Section(sign);
                nowSection.addPoint(lastPoint);
            }
            else{
                if(sign != nowSection.getSign()){
                    closeNowSection();

                    nowSection = null;
                    nowSection = new Section(sign);
                    nowSection.addPoint(lastPoint);
                }
            }
            nowSection.addPoint(point);
            lastPoint = point;
        }
        return false;
    }

    // variables getter and setter
    public void setThreshold(Double threshold){
        this.threshold = threshold;
    }

    public void setSmoothValue(Double smoothValue){
        this.smoothValue = smoothValue;
    }

    public void setWidthLimit(Double widthLimit){
        this.widthLimit = widthLimit;
    }

    public void setHeightLimit(Double heightLimit){
        this.heightLimit = heightLimit;
    }

    public void setType(String type){
        this.Type = type;
    }
}
