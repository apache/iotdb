package org.apache.iotdb.db.tools.logvisual;

import static org.apache.iotdb.db.tools.logvisual.VisualizePlan.PlanProperties.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.tools.logvisual.exceptions.UnmatchedContentException;

public class VisualizePlan {
  // optional, this will be used as the title of the figure.
  private String name;

  // required, a regex that will capture the logs to be analyzed, the interesting values
  // (measurements and group-by tags) should be surrounded with bracelets e.g.:
  //  contentPattern = location:(.*) temperature:(.*) pressure:(.*)
  private Pattern contentPattern;
  // the following 3 are optional, if not set, then it means this plan only cares whether this
  // event happens or not and draws a scatter plot, other wise it will capture the given
  // measurements and draw curves. Only numeric measurements are supported currently.
  // if one of first 2 is set, the other must be set
  // comma-separated
  // e.g.:
  //  measurementPositions = 2,3
  //  legends = temperature,pressure
  //  tagPositions = 1
  //  then the logs will be grouped-by their locations and for each group, there will be two
  //  curves describing temperature and pressure respectively
  private int[] measurementPositions;
  private String[] legends;
  private int[] tagPositions;

  private LogFilter logFilter;

  public VisualizePlan(Properties properties) throws IOException {
    this.name = properties.getProperty(NAME.getPropertyName(), "untitled");
    String patternStr = properties.getProperty(CONTENT_PATTERN.getPropertyName());
    if (patternStr == null) {
      throw new IOException("Bad plan, content pattern unset");
    }
    this.contentPattern = Pattern.compile(patternStr);

    String measurementPositionsStr = properties.getProperty(MEASUREMENT_POSITIONS.getPropertyName());
    if (measurementPositionsStr != null) {
      String[] measurePosStrs = measurementPositionsStr.split(",");
      measurementPositions = new int[measurePosStrs.length];
      for (int i = 0; i < measurementPositions.length; i++) {
        measurementPositions[i] = Integer.parseInt(measurePosStrs[i]);
      }
    }

    String legendStr = properties.getProperty(LEGENDS.getPropertyName());
    if (legendStr != null) {
      legends = legendStr.split(",");
    }

    String groupByPositionsStr = properties.getProperty(TAG_POSITIONS.getPropertyName());
    if (groupByPositionsStr != null) {
      String[] groupByPosStrs = groupByPositionsStr.split(",");
      tagPositions = new int[groupByPosStrs.length];
      for (int i = 0; i < tagPositions.length; i++) {
        tagPositions[i] = Integer.parseInt(groupByPosStrs[i]);
      }
    }

    logFilter = new LogFilter(properties);
  }

  public void parseContents(LogEntry logEntry) throws UnmatchedContentException {
    Matcher matcher = contentPattern.matcher(logEntry.getLogContent());
    if (!matcher.matches()) {
      throw new UnmatchedContentException(logEntry.getLogContent(), contentPattern.pattern());
    }
    String[] matchedValues = new String[matcher.groupCount()];
    for (int i = 1; i <= matcher.groupCount(); i++) {
      matchedValues[i - 1] = matcher.group(i);
    }
    if (tagPositions != null) {
      List<String> tags = new ArrayList<>();
      for (int pos : tagPositions) {
        tags.add(matchedValues[pos-1]);
      }
      logEntry.setTags(tags);
    }
    if (measurementPositions != null) {
      List<Double> measurements = new ArrayList<>();
      for (int pos : measurementPositions) {
        measurements.add(Double.parseDouble(matchedValues[pos-1]));
      }
      logEntry.setMeasurements(measurements);
    }
  }

  public String getName() {
    return name;
  }

  public Pattern getContentPattern() {
    return contentPattern;
  }

  public int[] getMeasurementPositions() {
    return measurementPositions;
  }

  public String[] getLegends() {
    return legends;
  }

  public int[] getTagPositions() {
    return tagPositions;
  }

  public LogFilter getLogFilter() {
    return logFilter;
  }

  enum PlanProperties {
    NAME("name"), CONTENT_PATTERN("content_pattern"), MEASUREMENT_POSITIONS(
        "measurement_positions"),
    LEGENDS("legends"), TAG_POSITIONS("tag_positions");

    private String propertyName;

    PlanProperties(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return propertyName;
    }
  }

  public static void main(String[] args) throws IOException {
    Properties properties = new Properties();
    properties.load(new FileInputStream("E:\\codestore\\incubator-iotdb\\server\\src\\assembly\\resources\\tools\\logAnalyze\\plans\\flushTimeConsumption.plan"));

    VisualizePlan plan = new VisualizePlan(properties);
  }
}