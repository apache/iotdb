/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.db.engine.modification;

/**
 * Modification represents an UPDATE or DELETE operation on a certain timeseries.
 */
public abstract class Modification {

  protected Type type;
  protected String path;
  protected long versionNum;

  public Modification(Type type, String path, long versionNum) {
    this.type = type;
    this.path = path;
    this.versionNum = versionNum;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public long getVersionNum() {
    return versionNum;
  }

  public void setVersionNum(long versionNum) {
    this.versionNum = versionNum;
  }

  public Type getType() {
    return type;
  }

  public void setType(Type type) {
    this.type = type;
  }

  public enum Type {
    DELETION
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Modification))
      return false;
    Modification mod = (Modification) obj;
    return mod.type.equals(this.type) && mod.path.equals(this.path)
            && mod.versionNum == this.versionNum;
  }
}
