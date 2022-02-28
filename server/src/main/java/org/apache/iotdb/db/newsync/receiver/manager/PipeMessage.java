package org.apache.iotdb.db.newsync.receiver.manager;

import java.util.Objects;

public class PipeMessage {
  private MsgType type;
  private String msg;

  public PipeMessage(MsgType msgType, String msg) {
    this.type = msgType;
    this.msg = msg;
  }

  public MsgType getType() {
    return type;
  }

  public void setType(MsgType type) {
    this.type = type;
  }

  public String getMsg() {
    return msg;
  }

  public void setMsg(String msg) {
    this.msg = msg;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PipeMessage that = (PipeMessage) o;
    return type == that.type && Objects.equals(msg, that.msg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, msg);
  }

  public enum MsgType {
    INFO,
    WARN,
    ERROR
  }
}
