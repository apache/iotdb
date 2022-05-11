package org.apache.iotdb.db.engine.trigger.sink.http;

import org.apache.iotdb.db.engine.trigger.sink.api.Event;

import com.google.gson.Gson;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HTTPEvent implements Event {
  private static final String PARAMETER_NULL_ERROR_STR = "parameter null error";

  // json 格式转发 还是 自定义格式
  private static final String formatter = "";
  private static final Pattern pattern = Pattern.compile("\\{\\{\\.\\w+}}");
  private final String fullPath;
  private final Map<String, String> labels;

  public HTTPEvent(String fullPath, Map<String, String> labels) {
    this.fullPath = fullPath;
    this.labels = labels;
  }

  public String toJsonString() {
    Gson gson = new Gson();
    //    Type gsonType = new TypeToken<Map>() {}.getType();
    //        StringBuilder sb = new StringBuilder();
    //        return sb.toString();

    return gson.toJson(this.labels);
  }

  private static String fillTemplate(Map<String, String> map, String template) {
    // TODO delete this method?
    if (template == null || map == null) {
      return null;
    }
    StringBuffer sb = new StringBuffer();
    Matcher m = pattern.matcher(template);
    while (m.find()) {
      String param = m.group();
      String key = param.substring(3, param.length() - 2).trim();
      String value = map.get(key);
      m.appendReplacement(sb, value == null ? "" : value);
    }
    m.appendTail(sb);
    return sb.toString();
  }

  public String getFullPath() {
    return fullPath;
  }
}
