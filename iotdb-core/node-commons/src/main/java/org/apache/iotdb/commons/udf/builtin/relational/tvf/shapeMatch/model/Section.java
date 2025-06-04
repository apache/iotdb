package org.apache.iotdb.commons.udf.builtin.relational.tvf.shapeMatch.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Section {
    private double sign; // enum -1, 0, 1
    private List<Point> points = new ArrayList<>();

    private Double MaxHeight = 0.0;
    private int MaxHeightIndex = 0;
    private Double MinHeight = Double.MAX_VALUE;
    private int MinHeightIndex = 0;
    private Double MaxWidth = 0.0;
    private Double MinWidth = Double.MAX_VALUE;

    private Double UpHeight = 0.0;

    private Boolean isFinal = false;
    private Boolean isVisited = false;

    private int id = 0;

    private Map<Integer,Double> calcResult = new HashMap<>();// only record the SE without divide the height(C)
    private List<Section> NextSectionList = new ArrayList<>();

    public Section(double sign) {
        this.sign = sign;
    }

    public void setSign(double sign) {
        this.sign = sign;
    }

    public double getSign() {
        return sign;
    }

    public void addPoint(Point point) {
        this.points.add(point);
        if (point.y > MaxHeight) {
            MaxHeight = point.y;
            MaxHeightIndex = points.size() - 1;
        }
        if (point.y < MinHeight) {
            MinHeight = point.y;
            MinHeightIndex = points.size() - 1;
        }
        if (point.x > MaxWidth) {
            MaxWidth = point.x;
        }
        if (point.x < MinWidth) {
            MinWidth = point.x;
        }
    }

    public List<Point> getPoints() {
        return points;
    }

    public Double getMaxHeight() {
        return MaxHeight;
    }

    public Double getMinHeight() {
        return MinHeight;
    }

    public Double getHeightBound() {
        return MaxHeight - MinHeight;
    }

    public int getMaxHeightIndex() {
        return MaxHeightIndex;
    }

    public int getMinHeightIndex() {
        return MinHeightIndex;
    }

    public Double getMaxWidth() {
        return MaxWidth;
    }

    public Double getMinWidth() {
        return MinWidth;
    }

    public Double getWidthBound() {
        return MaxWidth - MinWidth;
    }

    public List<Section> getNextSectionList() {
        return NextSectionList;
    }

    public void setUpHeight(Double upHeight) {
        this.UpHeight = upHeight;
    }

    public double getUpHeight() {
        return UpHeight;
    }

    public void setIsFinal(Boolean isFinal) {
        this.isFinal = isFinal;
    }

    public Boolean isFinal() {
        return isFinal;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setIsVisited(Boolean isVisited) {
        this.isVisited = isVisited;
    }

    public Boolean isVisited() {
        return isVisited;
    }

    public Map<Integer, Double> getCalcResult() {
        return calcResult;
    }

    public Section copy() {
        Section section = new Section(this.sign);
        for (Point point : this.points) {
            section.addPoint(new Point(point.x, point.y));
        }
        section.MaxHeight = this.MaxHeight;
        section.MinHeight = this.MinHeight;
        section.MaxWidth = this.MaxWidth;
        section.MinWidth = this.MinWidth;
        section.UpHeight = this.UpHeight;
        return section;
    }

    // address while reading data from database, only have sign, points , height, width
    public List<Section> concat(Section section, double smoothValue) {

        List<Section> concatResult = new ArrayList<>();

        double maxHeight = Math.max(this.MaxHeight, section.getMaxHeight());
        double minHeight = Math.min(this.MinHeight, section.getMinHeight());
        double bound = maxHeight - minHeight;
        if(bound <= smoothValue){
            for(int i = 1; i < section.getPoints().size(); i++){
                this.addPoint(section.getPoints().get(i));
            }
            concatResult.add(this);
        }
        else{
            Section firstSection = new Section(this.sign);
            Section midSection = null;
            Section secondSection = new Section(section.getSign());
            int indexFirst = 0;
            int indexSecond = 0;

            // if maxHeight > this.MaxHeight, claim that the second one is higher than the first one, so it is up state
            if(maxHeight > this.MaxHeight){
                // find the lower point which has the max x-axis in the first one, and the higher point which has a min x-axis in the second one
                // first one only save the first point to the minHeightIndex
                // mid one start from minHeightIndex to the end of the first one, and the first one to the maxHeightIndex in the second section
                // second one only save the maxHeightIndex to the end
                midSection = new Section(1);
                indexFirst = this.MinHeightIndex;
                indexSecond = section.getMaxHeightIndex();
            }
            // if not, and the bound is bigger claim the minHeight is lower than the first one, so it is down state
            else{
                // find the higher point which has the max x-axis in the first one, and the lower point which has a min x-axis in the second one
                // first one only save the first point to the maxHeightIndex
                // start from maxHeightIndex to the end of the first one, and the first one to the minHeightIndex in the second section
                // second one only save the minHeightIndex to the end
                midSection = new Section(-1);
                indexFirst = this.MaxHeightIndex;
                indexSecond = section.getMinHeightIndex();
            }

            for(int i = 0; i <= indexFirst; i++){
                firstSection.addPoint(this.points.get(i));
            }
            for(int i = indexFirst; i < this.points.size(); i++){
                midSection.addPoint(this.points.get(i));
            }
            for(int i = 1; i <= indexSecond; i++){
                midSection.addPoint(section.getPoints().get(i));
            }
            for(int i = indexSecond; i < section.getPoints().size(); i++){
                secondSection.addPoint(section.getPoints().get(i));
            }
            if(firstSection.getPoints().size() != 0){
                concatResult.add(firstSection);
            }
            concatResult.add(midSection);
            if(secondSection.getPoints().size() != 0){
                concatResult.add(secondSection);
            }
        }
        return concatResult;
    }

    // address while reading data from database, only have sign, points , height, width
    public void combine(Section section){
        for(int i = 1; i < section.getPoints().size(); i++){
            this.addPoint(section.getPoints().get(i));
        }
    }
}
