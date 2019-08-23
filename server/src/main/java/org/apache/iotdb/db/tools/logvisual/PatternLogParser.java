package org.apache.iotdb.db.tools.logvisual;

import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.CODE_LOCATION_INDEX;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.CONTENT_INDEX;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.DATE_INDEX;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.DATE_PATTERN;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.LEVEL_INDEX;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.PATTERN;
import static org.apache.iotdb.db.tools.logvisual.PatternLogParser.PatternProperties.THREAD_NAME_INDEX;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.db.tools.logvisual.LogEntry.CodeLocation;
import org.apache.iotdb.db.tools.logvisual.LogEntry.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PatternLogParser implements LogParser{
  private static final Logger logger = LoggerFactory.getLogger(LogParser.class);

  private Pattern pattern;
  private int dateIndex;
  private int threadNameIndex;
  private int levelIndex;
  private int codeLocationIndex;
  private int contentIndex;

  private String logFilePath;
  private BufferedReader reader;
  private DateFormat dateFormat;

  PatternLogParser(Properties properties, String logFilePath) throws IOException {
    this.pattern = Pattern.compile(properties.getProperty(PATTERN.getPropertyName()));
    this.dateIndex = Integer.parseInt(properties.getProperty(DATE_INDEX.getPropertyName()));
    this.threadNameIndex = Integer.parseInt(properties.getProperty(THREAD_NAME_INDEX
        .getPropertyName()));
    this.levelIndex = Integer.parseInt(properties.getProperty(LEVEL_INDEX.getPropertyName()));
    this.codeLocationIndex = Integer.parseInt(properties.getProperty(CODE_LOCATION_INDEX
        .getPropertyName()));
    this.contentIndex = Integer.parseInt(properties.getProperty(CONTENT_INDEX.getPropertyName()));
    this.dateFormat = new SimpleDateFormat(properties.getProperty(DATE_PATTERN.getPropertyName()));
    this.logFilePath = logFilePath;
  }

  @Override
  public LogEntry next() throws IOException {
    String line = reader.readLine();
    if (line == null) {
      return null;
    }

    Matcher matcher = pattern.matcher(line);
    if (!matcher.matches()) {
      logger.error("Unrecognizable log: {}", line);
      return null;
    }
    Date date;
    try {
      date = dateFormat.parse(matcher.group(dateIndex));
    } catch (ParseException e) {
      logger.error("Incorrect time format in {}", e);
      return null;
    }
    String threadName = matcher.group(threadNameIndex).trim();
    LogLevel logLevel = LogLevel.valueOf(matcher.group(levelIndex).trim());
    String[] codeLocationStr = matcher.group(codeLocationIndex).split(":");
    CodeLocation codeLocation = new CodeLocation(codeLocationStr[0].trim(), Integer.parseInt
        (codeLocationStr[1]));
    String content = matcher.group(contentIndex).trim();
    return new LogEntry(date, threadName, logLevel, codeLocation, content);
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }

  @Override
  public void reset() throws IOException {
    if (reader != null) {
      reader.close();
    }
    reader = new BufferedReader(new InputStreamReader(new FileInputStream(logFilePath)));
  }

  enum PatternProperties {
    PATTERN("pattern"), DATE_INDEX("date_index"), THREAD_NAME_INDEX("thread_name_index"),
    LEVEL_INDEX("level_index"), CODE_LOCATION_INDEX("code_location_index"), CONTENT_INDEX
        ("content_index"), DATE_PATTERN("date_pattern");

    private String propertyName;

    PatternProperties(String propertyName) {
      this.propertyName = propertyName;
    }

    public String getPropertyName() {
      return propertyName;
    }
  }
}