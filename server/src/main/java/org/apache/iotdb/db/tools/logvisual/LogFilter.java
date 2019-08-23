package org.apache.iotdb.db.tools.logvisual;

import static org.apache.iotdb.db.tools.logvisual.LogFilter.FilterProperties.*;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.DATE_PATTERN;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import org.apache.iotdb.db.tools.logvisual.LogEntry.LogLevel;

public class LogFilter {
  // optional, only logs with levels equal to or higher than this will be analyzed
  private LogLevel minLevel = LogLevel.DEBUG;
  // optional, only threads, classes, lines in the lists are analyzed. When unset, all logs will
  // analyzed. comma-separated
  private List<String> threadNameWhiteList;
  private List<String> classNameWhiteList;
  private List<Integer> lineNumWhiteList;
  // optional, only time ranges within the interval will be analyzed
  // if startDate or endDate is set, datePattern must be set too
  private DateFormat datePartten;
  private Date startDate = new Date(Long.MIN_VALUE);
  private Date endDate = new Date(Long.MAX_VALUE);

  public LogFilter(Properties properties) throws IOException {
    minLevel = LogLevel.valueOf(properties.getProperty(MIN_LEVEL.getPropertyName(), minLevel.name()));

    String threadNameWhiteListStr = properties.getProperty(THREAD_NAME_WHITE_LIST.getPropertyName
        ());
    if (threadNameWhiteListStr != null) {
      threadNameWhiteList = Arrays.asList(threadNameWhiteListStr.trim().split(","));
    }

    String classNameWhiteListStr = properties.getProperty(CLASS_NAME_WHITE_LIST.getPropertyName());
    if (classNameWhiteListStr != null) {
      classNameWhiteList = Arrays.asList(classNameWhiteListStr.trim().split(","));
    }

    String lineNumWhiteListStr = properties.getProperty(LINE_NUM_WHITE_LIST.getPropertyName());
    if (lineNumWhiteListStr != null) {
      String[] lineNumWhiteListStrs = lineNumWhiteListStr.trim().split(",");
      lineNumWhiteList = new ArrayList<>();
      for (int i = 0; i < lineNumWhiteListStrs.length; i++) {
        lineNumWhiteList.add(Integer.parseInt(lineNumWhiteListStrs[i]));
      }
    }

    String datePatternStr = properties.getProperty(DATE_PATTERN.getPropertyName());
    if (datePatternStr != null) {
      this.datePartten = new SimpleDateFormat(datePatternStr.trim());
      String startDateStr = properties.getProperty(START_DATE.getPropertyName());
      if (startDateStr != null) {
        try {
          startDate = datePartten.parse(startDateStr.trim());
        } catch (ParseException e) {
          throw new IOException(e);
        }
      }

      String endDatePattern = properties.getProperty(END_DATE.getPropertyName());
      if (startDateStr != null) {
        try {
          endDate = datePartten.parse(endDatePattern.trim());
        } catch (ParseException e) {
          throw new IOException(e);
        }
      }
    }
  }

  public FilterFeedBack filter(LogEntry entry) {
   if (entry.getLogLevel().ordinal() < minLevel.ordinal() ||
       (threadNameWhiteList != null && !threadNameWhiteList.contains(entry.getThreadName())) ||
       (classNameWhiteList != null && !classNameWhiteList.contains(entry.getCodeLocation()
           .getClassName())) ||
       (lineNumWhiteList != null && !lineNumWhiteList.contains(entry.getCodeLocation().getLineNum
           ())) ||
       (startDate != null && entry.getDate().before(startDate))) {
     return FilterFeedBack.REJECT;
   }

   if (endDate != null && entry.getDate().after(endDate)) {
     return FilterFeedBack.BEYOND_END_TIME;
   }
    return FilterFeedBack.OK;
  }

  enum FilterProperties {
    MIN_LEVEL("min_level"), THREAD_NAME_WHITE_LIST("thread_name_white_list"), CLASS_NAME_WHITE_LIST(
        "class_name_white_list"),
    LINE_NUM_WHITE_LIST("line_num_white_list"), START_DATE("start_date"), END_DATE("end_date"),
    DATE_PATTERN("date_pattern");

    private String propertyName;

    FilterProperties(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return propertyName;
    }
  }

  enum FilterFeedBack {
    OK, REJECT, BEYOND_END_TIME
  }
}