package org.apache.iotdb.commons.auth.user;

public class UserId {
  public final String strValue;
  public final Long longValue;

  private UserId(String str, Long lng) {
    this.strValue = str;
    this.longValue = lng;
  }

  public static UserId parse(String input) {
    try {
      return new UserId(null, Long.parseLong(input));
    } catch (NumberFormatException e) {
      return new UserId(input, null);
    }
  }

  public boolean isLong() {
    return longValue != null;
  }

  public boolean isString() {
    return strValue != null;
  }
}
