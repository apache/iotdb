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
package org.apache.iotdb.db.query.simpiece.jwave.datatypes.spaces;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.Super;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

/**
 * A space of Data; 3-D organized by (0,0,0) .. (noOfRows,noOfCols,noOfBlocks).
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 14:47:21
 */
public abstract class Space extends Super {

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:46:06
   */
  protected int _noOfRows;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:46:07
   */
  protected int _noOfCols;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:46:08
   */
  protected int _noOfLvls;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:23:47
   */
  protected int _offSetRow;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:23:49
   */
  protected int _offSetCol;

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:23:51
   */
  protected int _offSetLvl;

  /**
   * Create an object of a sub type; e.g. as pattern.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:28:20
   */
  public Space() {
    _offSetRow = 0;
    _offSetCol = 0;
    _offSetLvl = 0;
    _noOfRows = 0;
    _noOfCols = 0;
    _noOfLvls = 0;
  } // Space

  /**
   * Copy constructor passes the off sets and the dimesion of the passed space object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:29:35
   * @param space object of type space
   */
  public Space(Space space) {
    _offSetRow = space._offSetRow;
    _offSetCol = space._offSetCol;
    _offSetLvl = space._offSetLvl;
    _noOfRows = space._noOfRows;
    _noOfCols = space._noOfCols;
    _noOfLvls = space._noOfLvls;
  } // Space

  /**
   * Use this space as a single space that has all its off set at zero.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:47:21
   * @param noOfRows the number of rows
   * @param noOfCols the number of columns
   * @param noOfLvls the number of levels (height)
   */
  public Space(int noOfRows, int noOfCols, int noOfLvls) {
    _offSetRow = 0;
    _offSetCol = 0;
    _offSetLvl = 0;
    _noOfRows = noOfRows;
    _noOfCols = noOfCols;
    _noOfLvls = noOfLvls;
  } // Space

  /**
   * Configure a space (a cube) as a part of a super space.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:25:13
   * @param offSetRow the starting position for the row of the space
   * @param offSetCol the starting position for the column of the space
   * @param offSetLvl the starting position for the level (height) of the space
   * @param noOfRows the number of rows
   * @param noOfCols the number of columns
   * @param noOfLvls the number of levels (height)
   */
  public Space(
      int offSetRow, int offSetCol, int offSetLvl, int noOfRows, int noOfCols, int noOfLvls) {
    _offSetRow = offSetRow;
    _offSetCol = offSetCol;
    _offSetLvl = offSetLvl;
    _noOfRows = noOfRows;
    _noOfCols = noOfCols;
    _noOfLvls = noOfLvls;
  } // Space

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:47:27
   * @return the _noOfRows
   */
  public int getNoOfRows() {
    return _noOfRows;
  } // getNoOfRows

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:47:27
   * @return the _noOfCols
   */
  public int getNoOfCols() {
    return _noOfCols;
  } // getNoOfCols

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:47:27
   * @return the _noOfLvls
   */
  public int getNoOfLvls() {
    return _noOfLvls;
  } // getNoOfLvls

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:32:13
   * @return the off set of the row
   */
  public int getOffSetRow() {
    return _offSetRow;
  } // getOffSetRow

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:32:23
   * @return the off set of the column
   */
  public int getOffSetCol() {
    return _offSetCol;
  } // getOffSetCol

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:32:39
   * @return the off set of the level (height)
   */
  public int getOffSetLvl() {
    return _offSetLvl;
  } // getOffSetLvl

  /**
   * Check the given input of i as position in number of levels, otherwise throw a failure
   * (exception) if i as position is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:49:28
   * @param k from 0 to noOfLvls-1
   * @throws JWaveException if i is out of bounds
   */
  protected void check(int k) throws JWaveException {

    if (k < 0) throw new JWaveFailure("Space - i is smaller than zero");

    if (k == _noOfLvls) throw new JWaveFailure("Space - i is equal to noOfLvls: " + _noOfLvls);

    if (k > _noOfLvls) throw new JWaveFailure("Space - i is greater than noOfLvls: " + _noOfLvls);
  } // check

  /**
   * Check the given input of (j,k) and throws a failure (exception) if position is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:52:00
   * @param j from 0 to noOfCols-1
   * @param k from 0 to noOfLvls-1
   * @throws JWaveException if (j,k) is out of bounds
   */
  protected void check(int j, int k) throws JWaveException {

    if (j < 0) throw new JWaveFailure("Space - i is smaller than zero");

    if (j == _noOfCols) throw new JWaveFailure("Space - i is equal to noOfCols: " + _noOfCols);

    if (j > _noOfCols) throw new JWaveFailure("Space - i is greater than noOfCols: " + _noOfCols);

    check(k); // for checking k, while j is in every Block object stored.
  } // check

  /**
   * Check the given input of (i,j,k) and throws a failure (exception) if position is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:54:08
   * @param i from 0 to noOfRows-1
   * @param j from 0 to noOfCols-1
   * @param k from 0 to noOfLvls-1
   * @throws JWaveException if (i,j,k) is out of bounds
   */
  protected void check(int i, int j, int k) throws JWaveException {

    if (i < 0) throw new JWaveFailure("Space - i is smaller than zero");

    if (i == _noOfRows) throw new JWaveFailure("Space - i is equal to noOfRows: " + _noOfRows);

    if (i > _noOfRows) throw new JWaveFailure("Space - i is greater than noOfRows: " + _noOfRows);

    check(j, k); // for checking (j,k), while i is in every Line object stored.
  } // check

  /**
   * Getter!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:55:57
   * @param i from 0 to noOfRows-1
   * @param j from 0 to noOfCols-1
   * @param k from 0 to noOfLvls-1
   * @return the stored double value
   * @throws JWaveException if (i,j,k) is out of bounds
   */
  public abstract double get(int i, int j, int k) throws JWaveException;

  /**
   * Setter!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:56:24
   * @param i from 0 to noOfRows-1
   * @param j from 0 to noOfCols-1
   * @param k from 0 to noOfLvls-1
   * @param value any value of type double
   * @throws JWaveException if (i,j,k) is out of bounds
   */
  public abstract void set(int i, int j, int k, double value) throws JWaveException;
} // class
