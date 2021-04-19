/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.utils;

import java.util.ArrayList;
import java.util.List;

/**
 * this class is used to contact String effectively.It contains a StringBuider and initialize it
 * until {@code toString} is called. Note:it's not thread safety
 */
public class StringContainer {

  // while call toString, all substrings are jointed with joinSeparator
  private final String joinSeparator;
  private StringBuilder stringBuilder;
  private ArrayList<String> sequenceList;
  private ArrayList<String> reverseList;
  /** the summation length of all string segments. */
  private int totalLength = 0;
  /** the count of string segments. */
  private int count = 0;

  private boolean isUpdated = true;
  private String cache;

  /** defines the constructor function for the class. */
  public StringContainer() {
    sequenceList = new ArrayList<>();
    reverseList = new ArrayList<>();
    joinSeparator = null;
  }

  /** defines the constructor function for the class. */
  public StringContainer(String joinSeparator) {
    sequenceList = new ArrayList<>();
    reverseList = new ArrayList<>();
    this.joinSeparator = joinSeparator;
  }

  public StringContainer(String[] strings) {
    this();
    addTail(strings);
  }

  public StringContainer(String[] strings, String joinSeparator) {
    this(joinSeparator);
    addTail(strings);
  }

  public int size() {
    return count;
  }

  public int length() {
    return totalLength;
  }

  public List<String> getSequenceList() {
    return sequenceList;
  }

  public List<String> getReverseList() {
    return reverseList;
  }

  /**
   * add a objects array at this container's tail.
   *
   * @param objs -to be added
   * @return another string contains objs as tail
   */
  public StringContainer addTail(Object... objs) {
    isUpdated = true;
    count += objs.length;
    for (int i = 0; i < objs.length; i++) {
      String str = objs[i].toString();
      totalLength += str.length();
      sequenceList.add(str);
    }
    return this;
  }

  /**
   * add a Strings array at this container's tail.<br>
   * strings:"a","b","c",<br>
   * StringContainer this:["d","e","f"],<br>
   * result:this:["d","e","f","a","b","c"],<br>
   *
   * @param strings - to be added
   * @return - this object
   */
  public StringContainer addTail(String... strings) {
    isUpdated = true;
    count += strings.length;
    for (int i = 0; i < strings.length; i++) {
      totalLength += strings[i].length();
      sequenceList.add(strings[i]);
    }
    return this;
  }

  /**
   * add a StringContainer at this container's tail.<br>
   * param StringContainer:["a","b","c"],<br>
   * this StringContainer :["d","e","f"],<br>
   * result:this:["d","e","f","a","b","c"],<br>
   *
   * @param myContainer - to be added
   * @return - this object
   */
  public StringContainer addTail(StringContainer myContainer) {
    isUpdated = true;
    List<String> mySeqList = myContainer.getSequenceList();
    List<String> myRevList = myContainer.getReverseList();
    count += myRevList.size() + mySeqList.size();
    String temp;
    for (int i = myRevList.size() - 1; i >= 0; i--) {
      temp = myRevList.get(i);
      sequenceList.add(temp);
      totalLength += temp.length();
    }
    for (int i = 0; i < mySeqList.size(); i++) {
      temp = mySeqList.get(i);
      sequenceList.add(temp);
      totalLength += temp.length();
    }
    return this;
  }

  /**
   * add a Strings array from this container's header.<br>
   * strings:"a","b","c",<br>
   * StringContainer this:["d","e","f"],<br>
   * result:this:["a","b","c","d","e","f"],<br>
   *
   * @param strings - to be added
   * @return - this object
   */
  public StringContainer addHead(String... strings) {
    isUpdated = true;
    count += strings.length;
    for (int i = strings.length - 1; i >= 0; i--) {
      totalLength += strings[i].length();
      reverseList.add(strings[i]);
    }
    return this;
  }

  /**
   * add a StringContainer from this container's header.<br>
   * StringContainer m:["a","b","c"],<br>
   * StringContainer this:["d","e","f"],<br>
   * result:this:["a","b","c","d","e","f"],<br>
   *
   * @param myContainer - given StringContainer to be add in head
   * @return - this object
   */
  public StringContainer addHead(StringContainer myContainer) {
    isUpdated = true;
    List<String> mySeqList = myContainer.getSequenceList();
    List<String> myRevList = myContainer.getReverseList();
    count += myRevList.size() + mySeqList.size();
    String temp;
    for (int i = mySeqList.size() - 1; i >= 0; i--) {
      temp = mySeqList.get(i);
      reverseList.add(temp);
      totalLength += temp.length();
    }
    for (int i = 0; i < myRevList.size(); i++) {
      temp = myRevList.get(i);
      reverseList.add(temp);
      totalLength += temp.length();
    }
    return this;
  }

  @Override
  public String toString() {
    if (!isUpdated) {
      return cache;
    }
    if (totalLength <= 0) {
      return "";
    }
    if (joinSeparator == null) {
      stringBuilder = new StringBuilder(totalLength);
      for (int i = reverseList.size() - 1; i >= 0; i--) {
        stringBuilder.append(reverseList.get(i));
      }
      for (int i = 0; i < sequenceList.size(); i++) {
        stringBuilder.append(sequenceList.get(i));
      }
      cache = stringBuilder.toString();
    } else {
      cache = join(joinSeparator);
    }
    isUpdated = false;
    return cache;
  }

  /**
   * for all string in rev and seq, concat them with separator and return String.
   *
   * @param separator separator of string
   * @return - result joined in type of String with parameter
   */
  public String join(String separator) {
    if (totalLength <= 0) {
      return "";
    }
    stringBuilder = new StringBuilder(totalLength + (count - 1) * separator.length());
    for (int i = reverseList.size() - 1; i >= 1; i--) {
      stringBuilder.append(reverseList.get(i));
      stringBuilder.append(separator);
    }
    if (!reverseList.isEmpty()) {
      stringBuilder.append(reverseList.get(0));
      if (!sequenceList.isEmpty()) {
        stringBuilder.append(separator);
      }
    }
    int i;
    for (i = 0; i < sequenceList.size() - 1; i++) {
      stringBuilder.append(sequenceList.get(i));
      stringBuilder.append(separator);
    }
    if (!sequenceList.isEmpty()) {
      stringBuilder.append(sequenceList.get(i));
    }
    return stringBuilder.toString();
  }

  /**
   * return a sub-string in this container.<br>
   * e.g. this container is ["aa","bbb","cc","d","ee"]; this.getSubString(0) = "a";
   * this.getSubString(2) ="c";this.getSubString(-1) = "ee";
   *
   * @param index - the index of wanted sub-string
   * @return - substring result
   */
  public String getSubString(int index) {
    int realIndex = index >= 0 ? index : count + index;
    if (realIndex < 0 || realIndex >= count) {
      throw new IndexOutOfBoundsException(
          String.format("Index: %d, Real Index: %d, Size: %d", index, realIndex, count));
    }
    if (realIndex < reverseList.size()) {
      return reverseList.get(reverseList.size() - 1 - realIndex);
    } else {
      return sequenceList.get(realIndex - reverseList.size());
    }
  }

  /**
   * /** return a sub-container consist of several continuous strings in this {@code container.If
   * start <= end, return a empty container} e.g. this container is ["aa","bbb","cc","d","ee"];
   * this.getSubString(0,0) = ["aa"]<br>
   * this.getSubString(1,3) = ["bbb","cc","d"]<br>
   * this.getSubString(1,-1) = ["bbb","cc","d", "ee"]<br>
   *
   * @param start - the start index of wanted sub-string
   * @param end - the end index of wanted sub-string
   * @return - substring result
   */
  public StringContainer getSubStringContainer(int start, int end) {
    int realStartIndex = start >= 0 ? start : count + start;
    int realEndIndex = end >= 0 ? end : count + end;
    if (realStartIndex < 0 || realStartIndex >= count) {
      throw new IndexOutOfBoundsException(
          String.format(
              "start Index: %d, Real start Index: %d, Size: %d", start, realStartIndex, count));
    }
    if (realEndIndex < 0 || realEndIndex >= count) {
      throw new IndexOutOfBoundsException(
          String.format("end Index: %d, Real end Index: %d, Size: %d", end, realEndIndex, count));
    }
    StringContainer ret = new StringContainer(joinSeparator);
    if (realStartIndex < reverseList.size()) {
      for (int i = reverseList.size() - 1 - realStartIndex;
          i >= Math.max(0, reverseList.size() - 1 - realEndIndex);
          i--) {
        ret.addTail(this.reverseList.get(i));
      }
    }
    if (realEndIndex >= reverseList.size()) {
      for (int i = Math.max(0, realStartIndex - reverseList.size());
          i <= realEndIndex - reverseList.size();
          i++) {
        ret.addTail(this.sequenceList.get(i));
      }
    }
    return ret;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    if (joinSeparator != null) {
      result = prime * result + joinSeparator.hashCode();
    }
    for (String string : reverseList) {
      result = prime * result + ((string == null) ? 0 : string.hashCode());
    }
    for (String string : sequenceList) {
      result = prime * result + ((string == null) ? 0 : string.hashCode());
    }
    return result;
  }

  @Override
  public boolean equals(Object sc) {
    if (sc == null) return false;
    if (this.getClass() != sc.getClass()) return false;
    return this.equals((StringContainer) sc);
  }

  /**
   * judge whether the param is equal to this container.
   *
   * @param sc -StringContainer Object to judge whether the object is equal to this container
   * @return boolean value to judge whether is equal
   */
  public boolean equals(StringContainer sc) {
    if (sc == this) {
      return true;
    }
    if (count != sc.count) {
      return false;
    }
    if (totalLength != sc.totalLength) {
      return false;
    }
    if (!joinSeparator.equals(sc.joinSeparator)) {
      return false;
    }
    if (sequenceList.size() != sc.sequenceList.size()) {
      return false;
    }
    for (int i = 0; i < sequenceList.size(); i++) {
      if (!sequenceList.get(i).equals(sc.sequenceList.get(i))) {
        return false;
      }
    }
    if (reverseList.size() != sc.reverseList.size()) {
      return false;
    }
    for (int i = 0; i < reverseList.size(); i++) {
      if (!reverseList.get(i).equals(sc.reverseList.get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public StringContainer clone() {
    StringContainer ret = new StringContainer(joinSeparator);
    for (String s : sequenceList) {
      ret.sequenceList.add(s);
    }
    for (String s : reverseList) {
      ret.reverseList.add(s);
    }
    ret.totalLength = totalLength;
    ret.count = count;
    ret.isUpdated = isUpdated;
    ret.cache = cache;
    return ret;
  }
}
