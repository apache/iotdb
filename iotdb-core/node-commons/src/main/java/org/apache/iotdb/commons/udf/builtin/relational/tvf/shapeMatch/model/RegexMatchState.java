package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.calcSEusingMoreMemory;
import static org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.MatchConfig.shapeTolerance;

public class RegexMatchState {

    private Section patternSectionNow = null;

    private List<Section> dataSectionList = new ArrayList<>();

    // a stack to store the pattern path
    private Stack<Section> sectionStack = new Stack<>();

    private Stack<PathState> matchStateStack = new Stack<>();

    private List<PathState> matchResult = new ArrayList<>();

    public RegexMatchState(Section parentSectionNow) {
        this.patternSectionNow = parentSectionNow;
    }

    public List<PathState> getMatchResult() {
        return matchResult;
    }

    private void calcMatchValue(PathState pathState, double smoothValue, double threshold) {
        // calc the matchValue between sectionStack and dataSectionList, meta info is in pathState
        int index = 0;
        Iterator<Section> iterator = sectionStack.iterator();
        pathState.calcGlobalRadio();
        while (iterator.hasNext()) {
            Section patternSection = iterator.next();
            Section dataSection = dataSectionList.get(index);
            if(pathState.calcOneSectionMatchValue(patternSection,dataSection,smoothValue,threshold)){
                break;
            }
            index++;
        }
        if(index == dataSectionList.size()){
            matchResult.add(pathState);
        }

    }

    // return true claim that the pathState is finished, false claim that the pathState is alive
    public Boolean checkOneSectionInTopPathState(Section section, double heightLimit, double widthLimit, double smoothValue, double threshold) {
        // scan a start in one loop, and record the calc result in each section, and record the match path in each state

        // find the path match the sign in the section which not more than shapeTolerance

        // scan the dataSectionList. if the section pair no calc before, need to calc first and store. else the calc result is calc before, only need to update the Gx,Gy,Hc
        // while accumulate the result, need to check whether the result is in the threshold, if not, cut the branch and throw the result

        // the result only record the calc result distance, the start section, the length of the resultSectionList

        PathState pathState = matchStateStack.pop();
        sectionStack.push(pathState.getPatternSection());

        if(pathState.checkSign(section) && checkBoundLimit(pathState, heightLimit, widthLimit)){
            if(pathState.getPatternSection().isFinal()){
                calcMatchValue(pathState, smoothValue, threshold);
            }
            else{
                // trans to next state
                if(!pathState.getPatternSection().getNextSectionList().isEmpty()){
                    if(pathState.getPatternSection().getNextSectionList().size() == 1){
                        pathState.nextState(patternSectionNow.getNextSectionList().get(0));
                        matchStateStack.push(pathState);
                    }
                    else{
                        // copy the old pathState info and new dataSectionIndex, patternSection
                        // loop from the last one, the first one can be the top of the stack
                        for(int i = pathState.getPatternSection().getNextSectionList().size()-1; i >=0 ; i--) {
                            PathState newPathState = new PathState(pathState.getDataSectionIndex()+1, pathState.getPatternSection().getNextSectionList().get(i), pathState);
                            matchStateStack.push(newPathState);
                        }
                    }
                    return false;
                }
            }
        }
        return true;
    }

    // return true claim that the RegexMatchState is finish , false claim that regexMatchState is alive and wait for next section
    public Boolean addSection(Section section, double heightLimit, double widthLimit, double smoothValue, double threshold) {
        dataSectionList.add(section);
        if(matchStateStack.isEmpty()) {
            matchStateStack.push(new PathState(0, patternSectionNow));
        }

        if(checkOneSectionInTopPathState(section, heightLimit, widthLimit, smoothValue, threshold)){
            while(!matchStateStack.isEmpty() && matchStateStack.peek().getDataSectionIndex() < dataSectionList.size()){
                // update the top one's state
                PathState topPathState = matchStateStack.peek();
                // lose the top of the section until sectionStack size is smaller than the dataSectionIndex of the pathState
                while(topPathState.getDataSectionIndex() <= sectionStack.size()){
                    sectionStack.pop();
                }
                // loop the dataSectionList from the dataSectionIndex to the end
                for(int i = topPathState.dataSectionIndex; i < dataSectionList.size();i++){
                    if(checkOneSectionInTopPathState(dataSectionList.get(i), heightLimit, widthLimit, smoothValue, threshold)){
                        break;
                    }
                }
            }
        }

        // claim that start from this section has no case and run continue return true to trans to the next start.
        // while have some pathState in the stack, check the top one whether.
        // it's index point to the next of the end section of dataSectionList(matchStateStack.peek().getDataSectionIndex() == dataSectionList.size()) ,claim it is wait for next section
        return matchStateStack.isEmpty();
    }

    private Boolean checkBoundLimit(PathState pathState, double heightLimit, double widthLimit) {
        return (pathState.getDataHeightBound() <= heightLimit) && (pathState.getDataWidthBound() <= widthLimit);
    }

    private class PathState{
        private double matchValue = 0.0;

        private int dataSectionIndex = 0;
        private Section patternSection = null;

        private double TotalUpHeight = 0.0;

        private double dataMaxHeight = 0.0;
        private double dataMinHeight = Double.MAX_VALUE;
        private double dataMaxWidth = 0.0;
        private double dataMinWidth = Double.MAX_VALUE;

        private double patternMaxHeight = 0.0;
        private double patternMinHeight = Double.MAX_VALUE;
        private double patternMaxWidth = 0.0;
        private double patternMinWidth = Double.MAX_VALUE;

        private int shapeError = 0;

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

        public Boolean checkSign(Section section){
            if (section.getSign() == patternSection.getSign()) {
                updateDataBounds(section);
                return true;
            }
            shapeError++;
            if(shapeError <= shapeTolerance) {
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

            if (section.getMaxHeight()+TotalUpHeight > patternMaxHeight) {
                patternMaxHeight = section.getMaxHeight()+TotalUpHeight;
            }
            if (section.getMinHeight()+TotalUpHeight < patternMinHeight) {
                patternMinHeight = section.getMinHeight()+TotalUpHeight;
            }
            if (section.getMaxWidth() > patternMaxWidth) {
                patternMaxWidth = section.getMaxWidth();
            }
            if (section.getMinWidth() < patternMinWidth) {
                patternMinWidth = section.getMinWidth();
            }
        }

        private void copy(PathState pathState){
            this.TotalUpHeight = pathState.TotalUpHeight;

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

        public void calcGlobalRadio(){
            globalHeightRadio = (dataMaxHeight - dataMinHeight) / (patternMaxHeight - patternMinHeight);
            globalWitdhRadio = (dataMaxWidth - dataMinWidth) / (patternMaxWidth - patternMinWidth);
        }

        public Boolean calcOneSectionMatchValue(Section patternSection, Section dataSection, double smoothValue, double threshold) {
            // calc the LED
            // this is the Rx and Ry in the paper
            double localWidthRadio = dataSection.getWidthBound() / (patternSection.getWidthBound()*globalWitdhRadio);

            double localHeightUp = Math.max(dataSection.getHeightBound(), smoothValue);
            double localHeightDown = Math.max(patternSection.getHeightBound() * globalHeightRadio, smoothValue);
            double localHeightRadio = localHeightUp / localHeightDown;

            double LED = Math.pow(Math.log(localWidthRadio), 2) + Math.pow(Math.log(localHeightRadio), 2);

            // different way
            double shapeError = 0.0;
            if(calcSEusingMoreMemory && dataSection.getCalcResult().get(patternSection.getId()) != null){
                shapeError = dataSection.getCalcResult().get(patternSection.getId()) / (dataMaxHeight - dataMinHeight);
            }
            else{
                // calc the SE
                // align the first point or the centroid, it's same because the calculation is just an avg function, no matter where the align point is
                double alignHeightDiff = patternSection.getPoints().get(0).y * globalHeightRadio * localHeightRadio - dataSection.getPoints().get(0).y;

                // different from the origin code, in this case this code use linear fill technology to align the x-axis of all point in patternSection to dataSection
                int dataPointNum = dataSection.getPoints().size() - 1;
                int patternPointNum = patternSection.getPoints().size() - 1;

                double numRadio = ((double)dataPointNum) / ((double)patternPointNum);

                for(int i=1; i < dataPointNum; i++){
                    double patternIndex = i * numRadio;
                    int leftIndex = (int)patternIndex;
                    double leftRadio = patternIndex-leftIndex;
                    int rightIndex = leftIndex >= patternPointNum ? patternPointNum : leftIndex + 1;
                    double rightRadio = 1 - leftRadio;
                    // the heightValue is a weighted avg about the index near num
                    double pointHeight = patternSection.getPoints().get(leftIndex).y * rightRadio + patternSection.getPoints().get(rightIndex).y * leftRadio;

                    shapeError += pointHeight * globalHeightRadio * localHeightRadio - alignHeightDiff - dataSection.getPoints().get(i).y;
                }

                shapeError = shapeError / ((dataMaxHeight - dataMinHeight) * dataSection.getPoints().size());

                if(calcSEusingMoreMemory){
                    dataSection.getCalcResult().put(patternSection.getId(), shapeError*((dataMaxHeight - dataMinHeight)));
                }
            }

            matchValue = LED + shapeError;
            return matchValue > threshold;
        }
    }

}
