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

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.Super;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailureNotValid;

/**
 * A line of Data; 1-D organized by (0) .. (noOfRows).
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 15:00:01
 */
public abstract class Line extends Super {

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:02:58
   */
  protected int _noOfRows;

  /**
   * The position where the line start from as a sub set in the context of SuperLine. For example a
   * SuperLine Objects defines 0 .. 10 and by three Line objects this results in - e.g. - 0 .. 3 and
   * 4 .. 7 and 8 .. 9. the off sets of theses Line objects are: 0, 4, and 8.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 17:44:48
   */
  protected int _offSetRow;

  /**
   * Create an object of a sub type; e.g. as pattern.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 20:27:57
   */
  public Line() {
    _offSetRow = 0; // the block itself is global
    _noOfRows = 0;
  } // Line

  /**
   * Copy constructor - attention in base class there are only boundaries passed to the new object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 20.05.2015 07:23:56
   * @param line
   */
  public Line(Line line) {
    _offSetRow = line._noOfRows;
    _noOfRows = line._offSetRow;
  } // Line

  /**
   * Create a Line object by a certain number of rows.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:03:33
   * @param noOfRows the size or length of this Line object
   */
  public Line(int noOfRows) {
    _offSetRow = 0; // the block itself is global
    _noOfRows = noOfRows;
  } // Line

  /**
   * Create a Line object by an off set and the number of rows. Set the off set of row starting for
   * this line; e.g. _offSetRow + _noOfRows => next_offSetRow of next_lineObject!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 17:54:29
   * @param offSetRow the global off set of this Line object in a SuperLine object
   * @param noOfRows the size or length of this Line object
   */
  public Line(int offSetRow, int noOfRows) {
    _offSetRow = offSetRow;
    _noOfRows = noOfRows;
  } // Line

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:06:52
   * @return the _noOfRows
   */
  public int getNoOfRows() {
    return _noOfRows;
  } // getNoOfRows

  public void setNoOfRows(int noOfRows) throws JWaveException {

    if (isAllocated())
      throw new JWaveFailureNotValid(
          "Line - "
              + "setting new number of rows is not valid "
              + "while memory is already allocated");

    _noOfRows = noOfRows;
  } // setNoOfRows

  /**
   * Getter for the global off set of this Line object in a SuperLine object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 17:52:00
   * @return the global off set of this Line object in a SuperLine object
   */
  public int getOffSetRow() {
    return _offSetRow;
  } // getOffSetRow

  /**
   * Set the off set of row starting for this line; e.g. _offSetRow + _noOfRows => next_offSetRow of
   * next_lineObject!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 18.05.2015 17:51:02
   * @param offSetRows the global off set of this Line object in a SuperLine object
   */
  protected void setOffSetRow(int offSetRow) { // set to protected due to miss usage
    _offSetRow = offSetRow;
  } // setOffSetRow

  /**
   * Check the given input of i as position in number of rows, otherwise throw a failure (exception)
   * if i as position is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:13:18
   * @param i from 0 to noOfRows-1
   * @throws JWaveException if i is out of bounds
   */
  protected void checkIndex(int i) throws JWaveException {

    if (i < 0) throw new JWaveFailureNotValid("Line - i is smaller than zero");

    if (i == _noOfRows)
      throw new JWaveFailureNotValid("Line - i is equal to noOfRows: " + _noOfRows);

    if (i > _noOfRows)
      throw new JWaveFailureNotValid("Line - i is greater than noOfRows: " + _noOfRows);
  } // checkIndex

  /**
   * Getter for the stored values.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:10:04
   * @param pos from 0 to noOfRows-1 as range of input
   * @return the stored double value
   * @throws JWaveException if i is out of bounds
   */
  public abstract double get(int i) throws JWaveException;

  /**
   * Setter to store values.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:16:42
   * @param i from 0 to noOfRows-1 as range of input
   * @param val the value to be stored in Line object.
   * @throws JWaveException if i out of bounds
   */
  public abstract void set(int i, double val) throws JWaveException;
} // class
