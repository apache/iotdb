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
package org.apache.iotdb.db.query.simpiece.jwave.datatypes.blocks;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.Super;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

/**
 * A block of Data; 2-D organized by (0,0) .. (noOfRows,noOfCols). This object is strictly using the
 * Line objects, due to having here the possibility to implement different strategies on values
 * storage.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 14:39:22
 */
public abstract class Block extends Super {

  /**
   * The number of rows of this Block.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:41:23
   */
  protected int _noOfRows;

  /**
   * The number of columns of this Block.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:41:42
   */
  protected int _noOfCols;

  /**
   * The row position where the block starts from as a sub set in the context of SuperBlock; check
   * _offSetRow in class Line for more details.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 23.05.2015 19:50:40
   */
  protected int _offSetRow;

  /**
   * The column position where the block starts from as a sub set in the context of SuperBlock;
   * check _offSetRow in class Line for more details.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 23.05.2015 19:52:19
   */
  protected int _offSetCol;

  /**
   * Create an object of a sub type; e.g. as pattern.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 23.05.2015 19:53:18
   */
  public Block() {
    _offSetRow = 0;
    _offSetCol = 0;
    _noOfRows = 0;
    _noOfCols = 0;
  } // Block

  /**
   * Copy constructor - attention in base class there are only boundaries passed to the new object.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 23.05.2015 19:54:16
   * @param block
   */
  public Block(Block block) {
    _offSetRow = block._offSetRow;
    _offSetCol = block._offSetCol;
    _noOfRows = block._offSetRow;
    _noOfCols = block._noOfCols;
  } // Block

  /**
   * Taking the block object as a single global object by setting both off sets to zero.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:21:17
   * @param noOfRows stores the noOfRows exclusively, even if each Line object stores that value
   *     again.
   * @param noOfCols the no of columns or the number of Line objects.
   */
  public Block(int noOfRows, int noOfCols) {
    _offSetRow = 0;
    _offSetCol = 0;
    _noOfRows = noOfRows;
    _noOfCols = noOfCols;
  } // Block

  /**
   * Passing information that takes the block as a part of a global structure; e.g. a SuperBlock.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 14:03:45
   * @param offSetRow the global off set of rows of the block
   * @param offSetCol the global off set of columns of the block
   * @param noOfRows the number of rows of the block
   * @param noOfCols the number of columns of the block
   */
  public Block(int offSetRow, int offSetCol, int noOfRows, int noOfCols) {
    _offSetRow = offSetRow;
    _offSetCol = offSetCol;
    _noOfRows = noOfRows;
    _noOfCols = noOfCols;
  } // Block

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:43:41
   * @return the _noOfRows
   */
  public int getNoOfRows() {
    return _noOfRows;
  } // getNoOfRows

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:43:41
   * @return the _noOfCols
   */
  public int getNoOfCols() {
    return _noOfCols;
  } // getNoOfCols

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:31:31
   * @return the off set of the row
   */
  public int getOffSetRow() {
    return _offSetRow;
  } // getOffSetRow

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 18:31:54
   * @return the off set of the column
   */
  public int getOffSetCol() {
    return _offSetCol;
  } // getOffSetCol

  /**
   * Check the given input of j is in bound of the number of columns, otherwise throw a failure
   * (exception) if j is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:37:57
   * @param j from 0 to noOfCols-1
   * @throws JWaveException
   */
  protected void check(int j) throws JWaveException {

    if (j < 0) throw new JWaveFailure("Block - i is smaller than zero");

    if (j == _noOfCols) throw new JWaveFailure("Block - i is equal to noOfCols: " + _noOfCols);

    if (j > _noOfCols) throw new JWaveFailure("Block - i is greater than noOfCols: " + _noOfCols);
  } // check

  /**
   * Check the given input of (i,j) and throws a failure (exception) if position is not valid.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:33:14
   * @param i from 0 to noOfRows-1
   * @param j from 0 to noOfCols-1
   * @throws JWaveException if (i,j) is out of bounds
   */
  protected void check(int i, int j) throws JWaveException {

    if (i < 0) throw new JWaveFailure("Block - i is smaller than zero");

    if (i == _noOfRows) throw new JWaveFailure("Block - i is equal to noOfRows: " + _noOfRows);

    if (i > _noOfRows) throw new JWaveFailure("Block - i is greater than noOfRows: " + _noOfRows);

    check(j); // for checking j, while i is in every Line object stored.
  } // check

  /**
   * Getter for a stored value of type double.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:23:22
   * @param i position in rows of the block
   * @param j position in columns of the block
   * @return the stored value
   * @throws JWaveException if i and / or j are out of bounds
   */
  public abstract double get(int i, int j) throws JWaveException;

  /**
   * Setter for a stored value of type double!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:42:53
   * @param i position in rows of the block
   * @param j position in columns of the block
   * @param value any value in range of a double type is possible
   * @throws JWaveException if i and / or j are out of bounds
   */
  public abstract void set(int i, int j, double value) throws JWaveException;
} // class
