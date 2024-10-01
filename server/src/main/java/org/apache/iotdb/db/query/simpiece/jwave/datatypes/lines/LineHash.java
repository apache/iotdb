/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines;

import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailureNotFound;

import java.util.HashMap;

/**
 * Uses HashMap generic for sparse data representations.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 16:30:00
 */
public class LineHash extends Line {

  /**
   * Simple hash mapping for sparse data.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:31:41
   */
  HashMap<Integer, Double> _hashMap;

  /**
   * Pass nothing, use this a a place holder.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:41:15
   */
  public LineHash() {
    super();
  } // LineHash

  /**
   * Copy constructor that takes over - if available - the values of another type of Line object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.05.2015 07:28:18
   * @param line an object of type Line
   */
  public LineHash(Line line) {

    super(line); // takes the number of rows and the off set value

    try {

      alloc();

      for (int i = 0; i < line._noOfRows; i++) {
        set(i, line.get(i));
      }

    } catch (JWaveException e) {
      e.printStackTrace();
    } // try

    // TODO improve constructor memory passing by (instance of)

  } // LineHash

  /**
   * Pass the number of rows - global line?!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:30:00
   * @param noOfRows
   */
  public LineHash(int noOfRows) {
    super(noOfRows);
  } // LineHash

  /**
   * Pass an of set to the line and a number of rows.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:40:38
   * @param offSetRow
   * @param noOfRows
   */
  public LineHash(int offSetRow, int noOfRows) {
    super(offSetRow, noOfRows);
  } // LineHash

  /*
   * Get a full copy of this Line object!
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 21:02:37 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#copy()
   */
  @Override
  public Line copy() {

    int offSetRow = getOffSetRow();
    int noOfRows = getNoOfRows();

    Line line = new LineHash(offSetRow, noOfRows);

    try {
      if (isAllocated()) {
        line.alloc();
        for (int i = 0; i < noOfRows; i++) {
          try {
            line.set(i, get(i));
          } catch (JWaveFailureNotFound notFound) {
            // do nothing, no value stored
          } catch (JWaveException e) {
            e.printStackTrace();
          } // try - never ever
        } // i
      } // isAllocated
    } catch (JWaveException e) {
      e.printStackTrace();
    } // try - never ever

    return line;
  } // copy

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 18:11:28 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#isAllocated()
   */
  @Override
  public boolean isAllocated() {
    boolean isAllocated = true;
    if (_hashMap == null) {
      isAllocated = false;
    }
    return isAllocated;
  } // isAllocated

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 18:12:03 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#alloc()
   */
  @Override
  public void alloc() throws JWaveException {

    if (!isAllocated()) {
      _hashMap = new HashMap<Integer, Double>();
    }
  } // alloc

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 18:12:13 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#erase()
   */
  @Override
  public void erase() throws JWaveException {

    _hashMap = null;
  } // erase

  /*
   * Getter!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:30:00 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#get(int)
   */
  @Override
  public double get(int i) throws JWaveException {

    checkMemory();

    checkIndex(i);

    double value = 0.;

    if (_hashMap.containsKey(i)) {
      value = _hashMap.get(i);
    } else {
      throw new JWaveFailureNotFound("Line - no value stored for requested i: " + i);
    }

    return value;
  } // get

  /*
   * Setter!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:30:00 (non-Javadoc)
   * @see jwave.datatypes.lines.Line#set(int, double)
   */
  @Override
  public void set(int i, double value) throws JWaveException {

    checkMemory();

    checkIndex(i);

    _hashMap.put(i, value);
  } // set
} // class
