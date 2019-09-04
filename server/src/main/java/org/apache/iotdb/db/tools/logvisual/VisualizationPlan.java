package org.apache.iotdb.db.tools.logvisual;

import static org.apache.iotdb.db.tools.logvisual.VisualizationPlan.PlanProperties.*;

import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.tools.logvisual.exceptions.UnmatchedContentException;

/**
 * VisualizationPlan defines what fields (by using groups in regexp) should be plotted as value,
 * what fields should be used to group the logs and what logs should be filtered.
 * An example plan:
    name=flushTimeConsumption
    content_pattern=Storage group (.*) memtable (.*) flushing a memtable has finished! Time consumption: (.*)ms

    measurement_positions=3
    legends=Time
    tag_positions=1

    min_level=INFO
    thread_name_white_list=pool-1-IoTDB-JDBC-Client-thread-5
    class_name_white_list=org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor
    line_num_white_list=392

    date_pattern=yyyy-MM-dd hh:mm:ss
    start_date=2019-08-21 09:00:00
    end_date=2019-08-22 09:00:00

    TODO-LogVisualizer: support expression computation like; sum(measurement1 * measurement2)
    TODO-LogVisualizer: add tag filter
 */
public class VisualizationPlan {
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

  // where the plan is stored
  private String planFilePath;

  public VisualizationPlan() {
    logFilter = new LogFilter();
  }

  VisualizationPlan(String planFilePath) throws IOException {
    this.planFilePath = planFilePath;
    Properties properties = new Properties();
    try (FileInputStream reader = new FileInputStream(planFilePath);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(reader)) {
      properties.load(bufferedInputStream);
    }

    this.name = properties.getProperty(NAME.getPropertyName(), "untitled");
    String patternStr = properties.getProperty(CONTENT_PATTERN.getPropertyName());
    if (patternStr == null) {
      throw new IOException("Bad plan, content pattern unset");
    }
    this.contentPattern = Pattern.compile(patternStr);

    measurementPositions = VisualUtils.parseIntArray(properties.getProperty(MEASUREMENT_POSITIONS
        .getPropertyName()));

    String legendStr = properties.getProperty(LEGENDS.getPropertyName());
    if (legendStr != null) {
      legends = legendStr.split(",");
    }

    tagPositions = VisualUtils.parseIntArray(properties.getProperty(TAG_POSITIONS.getPropertyName()));

    logFilter = new LogFilter(properties);
  }

  /**
   * parse the content in a LogEntry using contentPattern and store the parsed fields back to the
   * entry.
   * @param logEntry
   * @throws UnmatchedContentException
   */
  public void parseContents(LogEntry logEntry) throws UnmatchedContentException {
    Matcher matcher = contentPattern.matcher(logEntry.getLogContent());
    if (!matcher.matches()) {
      throw new UnmatchedContentException(logEntry.getLogContent(), contentPattern.pattern());
    }

    String[] matchedValues = new String[matcher.groupCount()];
    for (int i = 1; i <= matcher.groupCount(); i++) {
      // group(0) is the whole string
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

  public void setName(String name) {
    this.name = name;
  }

  public void setContentPattern(Pattern contentPattern) {
    this.contentPattern = contentPattern;
  }

  public void setMeasurementPositions(int[] measurementPositions) {
    this.measurementPositions = measurementPositions;
  }

  public void setLegends(String[] legends) {
    this.legends = legends;
  }

  public void setTagPositions(int[] tagPositions) {
    this.tagPositions = tagPositions;
  }
  
  public void setPlanFilePath(String planFilePath) {
    this.planFilePath = planFilePath;
  }

  public String getPlanFilePath() {
    return planFilePath;
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

  @Override
  public String toString() {
    return name;
  }

  public void saveAsFile() throws IOException {
    Properties properties = saveAsProperties();
    logFilter.saveIntoProperties(properties);
    try (FileWriter fileWriter = new FileWriter(planFilePath);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {
      properties.store(bufferedWriter, "");
    }
  }

  private Properties saveAsProperties() {
    Properties properties = new Properties();
    properties.put(PlanProperties.NAME.getPropertyName(), name);
    properties.put(PlanProperties.CONTENT_PATTERN.getPropertyName(), contentPattern.pattern());
    if (measurementPositions != null) {
      properties.put(PlanProperties.MEASUREMENT_POSITIONS.getPropertyName(), VisualUtils.intArrayToString
          (measurementPositions));
    }
    if (legends != null) {
      properties.put(PlanProperties.LEGENDS.getPropertyName(), String.join(",", legends));
    }
    if (tagPositions != null) {
      properties.put(PlanProperties.TAG_POSITIONS.getPropertyName(), VisualUtils.intArrayToString(tagPositions));
    }
    return properties;
  }
}