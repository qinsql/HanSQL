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
package org.lealone.hansql.optimizer.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.lealone.hansql.optimizer.rel.metadata.Metadata;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.AllPredicates;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Collation;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.ColumnUniqueness;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.CumulativeCost;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.DistinctRowCount;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Distribution;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.ExplainVisibility;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.ExpressionLineage;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.MaxRowCount;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Memory;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.MinRowCount;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.NodeTypes;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Parallelism;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.PercentageOriginalRows;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.PopulationSize;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Predicates;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.RowCount;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Selectivity;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.Size;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.TableReferences;
import org.lealone.hansql.optimizer.rel.metadata.BuiltInMetadata.UniqueKeys;
import org.lealone.hansql.optimizer.rel.type.java.Types;
import org.lealone.hansql.optimizer.rex.RexNode;
import org.lealone.hansql.optimizer.sql.SqlExplainLevel;

import com.google.common.collect.ImmutableMap;

/**
 * Built-in methods.
 */
public enum BuiltInMethod {
    OBJECT_TO_STRING(Object.class, "toString"),
    SELECTIVITY(Selectivity.class, "getSelectivity", RexNode.class),
    UNIQUE_KEYS(UniqueKeys.class, "getUniqueKeys", boolean.class),
    AVERAGE_ROW_SIZE(Size.class, "averageRowSize"),
    AVERAGE_COLUMN_SIZES(Size.class, "averageColumnSizes"),
    IS_PHASE_TRANSITION(Parallelism.class, "isPhaseTransition"),
    SPLIT_COUNT(Parallelism.class, "splitCount"),
    MEMORY(Memory.class, "memory"),
    CUMULATIVE_MEMORY_WITHIN_PHASE(Memory.class, "cumulativeMemoryWithinPhase"),
    CUMULATIVE_MEMORY_WITHIN_PHASE_SPLIT(Memory.class, "cumulativeMemoryWithinPhaseSplit"),
    COLUMN_UNIQUENESS(ColumnUniqueness.class, "areColumnsUnique", ImmutableBitSet.class, boolean.class),
    COLLATIONS(Collation.class, "collations"),
    DISTRIBUTION(Distribution.class, "distribution"),
    NODE_TYPES(NodeTypes.class, "getNodeTypes"),
    ROW_COUNT(RowCount.class, "getRowCount"),
    MAX_ROW_COUNT(MaxRowCount.class, "getMaxRowCount"),
    MIN_ROW_COUNT(MinRowCount.class, "getMinRowCount"),
    DISTINCT_ROW_COUNT(DistinctRowCount.class, "getDistinctRowCount", ImmutableBitSet.class, RexNode.class),
    PERCENTAGE_ORIGINAL_ROWS(PercentageOriginalRows.class, "getPercentageOriginalRows"),
    POPULATION_SIZE(PopulationSize.class, "getPopulationSize", ImmutableBitSet.class),
    COLUMN_ORIGIN(ColumnOrigin.class, "getColumnOrigins", int.class),
    EXPRESSION_LINEAGE(ExpressionLineage.class, "getExpressionLineage", RexNode.class),
    TABLE_REFERENCES(TableReferences.class, "getTableReferences"),
    CUMULATIVE_COST(CumulativeCost.class, "getCumulativeCost"),
    NON_CUMULATIVE_COST(NonCumulativeCost.class, "getNonCumulativeCost"),
    PREDICATES(Predicates.class, "getPredicates"),
    ALL_PREDICATES(AllPredicates.class, "getAllPredicates"),
    EXPLAIN_VISIBILITY(ExplainVisibility.class, "isVisibleInExplain", SqlExplainLevel.class),
    METADATA_REL(Metadata.class, "rel"),;

    public final Method method;
    public final Constructor<?> constructor;
    public final Field field;

    public static final ImmutableMap<Method, BuiltInMethod> MAP;

    static {
        final ImmutableMap.Builder<Method, BuiltInMethod> builder = ImmutableMap.builder();
        for (BuiltInMethod value : BuiltInMethod.values()) {
            if (value.method != null) {
                builder.put(value.method, value);
            }
        }
        MAP = builder.build();
    }

    BuiltInMethod(Method method, Constructor<?> constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /** Defines a method. */
    BuiltInMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    public String getMethodName() {
        return method.getName();
    }
}

// End BuiltInMethod.java
