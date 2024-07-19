/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.queryengine.plan.relational.planner.assertions;

import org.apache.iotdb.db.queryengine.common.SessionInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNode;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.planner.Symbol;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/** Just check alias is present; return self mapping from symbolAliasesMap. */
class AliasPresent implements RvalueMatcher {
  private final String alias;

  AliasPresent(String alias) {
    this.alias = requireNonNull(alias, "alias cannot be null");
  }

  @Override
  public Optional<Symbol> getAssignedSymbol(
      PlanNode node, SessionInfo sessionInfo, Metadata metadata, SymbolAliases symbolAliases) {
    return symbolAliases.getOptional(alias).map(Symbol::from);
  }

  @Override
  public String toString() {
    return "has " + alias;
  }
}
