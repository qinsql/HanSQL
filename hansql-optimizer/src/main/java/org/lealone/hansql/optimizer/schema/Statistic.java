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

import java.util.List;

import org.lealone.hansql.optimizer.rel.RelCollation;
import org.lealone.hansql.optimizer.rel.RelDistribution;
import org.lealone.hansql.optimizer.rel.RelReferentialConstraint;
import org.lealone.hansql.optimizer.util.ImmutableBitSet;

/**
 * Statistics about a {@link Table}.
 *
 * <p>Each of the methods may return {@code null} meaning "not known".</p>
 *
 * @see Statistics
 */
public interface Statistic {
  /** Returns the approximate number of rows in the table. */
  Double getRowCount();

  /** Returns whether the given set of columns is a unique key, or a superset
   * of a unique key, of the table.
   */
  boolean isKey(ImmutableBitSet columns);

  /** Returns the collection of referential constraints (foreign-keys)
   * for this table. */
  List<RelReferentialConstraint> getReferentialConstraints();

  /** Returns the collections of columns on which this table is sorted. */
  List<RelCollation> getCollations();

  /** Returns the distribution of the data in this table. */
  RelDistribution getDistribution();
}

// End Statistic.java