/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.optimizer.schema;

import java.util.Collection;
import java.util.List;

import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptTable;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.core.TableModify;
import org.lealone.hansql.optimizer.rex.RexNode;

/**
 * A table that can be modified.
 *
 * <p>NOTE: The current API is inefficient and experimental. It will change
 * without notice.</p>
 *
 * @see ModifiableView
 */
public interface ModifiableTable extends QueryableTable {
  /** Returns the modifiable collection.
   * Modifying the collection will change the table's contents. */
  Collection<?> getModifiableCollection();

  /** Creates a relational expression that modifies this table. */
  TableModify toModificationRel(
      RelOptCluster cluster,
      RelOptTable table,
      CatalogReader catalogReader,
      RelNode child,
      TableModify.Operation operation,
      List<String> updateColumnList,
      List<RexNode> sourceExpressionList,
      boolean flattened);
}

// End ModifiableTable.java
