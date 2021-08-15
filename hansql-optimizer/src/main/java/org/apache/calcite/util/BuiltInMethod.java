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
package org.apache.calcite.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;

import org.apache.calcite.rel.metadata.BuiltInMetadata.AllPredicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Collation;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnOrigin;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ColumnUniqueness;
import org.apache.calcite.rel.metadata.BuiltInMetadata.CumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.DistinctRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Distribution;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExplainVisibility;
import org.apache.calcite.rel.metadata.BuiltInMetadata.ExpressionLineage;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MaxRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Memory;
import org.apache.calcite.rel.metadata.BuiltInMetadata.MinRowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NodeTypes;
import org.apache.calcite.rel.metadata.BuiltInMetadata.NonCumulativeCost;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Parallelism;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PercentageOriginalRows;
import org.apache.calcite.rel.metadata.BuiltInMetadata.PopulationSize;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Predicates;
import org.apache.calcite.rel.metadata.BuiltInMetadata.RowCount;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Selectivity;
import org.apache.calcite.rel.metadata.BuiltInMetadata.Size;
import org.apache.calcite.rel.metadata.BuiltInMetadata.TableReferences;
import org.apache.calcite.rel.metadata.BuiltInMetadata.UniqueKeys;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.type.java.Primitive;
import org.apache.calcite.rel.type.java.Types;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.util.DataContext;
import org.apache.calcite.util.function.Function1;

import com.google.common.collect.ImmutableMap;

/**
 * Built-in methods.
 */
public enum BuiltInMethod {
    SCHEMA_GET_SUB_SCHEMA(Schema.class, "getSubSchema", String.class),
    SCHEMA_GET_TABLE(Schema.class, "getTable", String.class),
    SCHEMA_PLUS_UNWRAP(SchemaPlus.class, "unwrap", Class.class),
    SCHEMAS_ENUMERABLE_SCANNABLE(Schemas.class, "enumerable", ScannableTable.class, DataContext.class),
    SCHEMAS_ENUMERABLE_FILTERABLE(Schemas.class, "enumerable", FilterableTable.class, DataContext.class),
    DATA_CONTEXT_GET(DataContext.class, "get", String.class),
    DATA_CONTEXT_GET_ROOT_SCHEMA(DataContext.class, "getRootSchema"),
    ROW_VALUE(Row.class, "getObject", int.class),
    ROW_AS_COPY(Row.class, "asCopy", Object[].class),
    FUNCTION1_APPLY(Function1.class, "apply", Object.class),
    ARRAYS_AS_LIST(Arrays.class, "asList", Object[].class),
    LIST_N(FlatLists.class, "copyOf", Comparable[].class),
    LIST2(FlatLists.class, "of", Object.class, Object.class),
    LIST3(FlatLists.class, "of", Object.class, Object.class, Object.class),
    LIST4(FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class),
    LIST5(FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class, Object.class),
    LIST6(FlatLists.class, "of", Object.class, Object.class, Object.class, Object.class, Object.class, Object.class),
    COMPARABLE_EMPTY_LIST(FlatLists.class, "COMPARABLE_EMPTY_LIST", true),
    AS_LIST(Primitive.class, "asList", Object.class),
    ENUMERATOR_CURRENT(Enumerator.class, "current"),
    ENUMERATOR_MOVE_NEXT(Enumerator.class, "moveNext"),
    ENUMERATOR_CLOSE(Enumerator.class, "close"),
    ENUMERATOR_RESET(Enumerator.class, "reset"),
    RESULT_SET_GET_DATE2(ResultSet.class, "getDate", int.class, Calendar.class),
    RESULT_SET_GET_TIME2(ResultSet.class, "getTime", int.class, Calendar.class),
    RESULT_SET_GET_TIMESTAMP2(ResultSet.class, "getTimestamp", int.class, Calendar.class),
    TIME_ZONE_GET_OFFSET(TimeZone.class, "getOffset", long.class),
    LONG_VALUE(Number.class, "longValue"),
    COMPARATOR_COMPARE(Comparator.class, "compare", Object.class, Object.class),
    COLLECTIONS_REVERSE_ORDER(Collections.class, "reverseOrder"),
    COLLECTIONS_EMPTY_LIST(Collections.class, "emptyList"),
    COLLECTIONS_SINGLETON_LIST(Collections.class, "singletonList", Object.class),
    COLLECTION_SIZE(Collection.class, "size"),
    MAP_CLEAR(Map.class, "clear"),
    MAP_GET(Map.class, "get", Object.class),
    MAP_PUT(Map.class, "put", Object.class, Object.class),
    COLLECTION_ADD(Collection.class, "add", Object.class),
    COLLECTION_ADDALL(Collection.class, "addAll", Collection.class),
    LIST_GET(List.class, "get", int.class),
    ITERATOR_HAS_NEXT(Iterator.class, "hasNext"),
    ITERATOR_NEXT(Iterator.class, "next"),
    MATH_MAX(Math.class, "max", int.class, int.class),
    MATH_MIN(Math.class, "min", int.class, int.class),
    FLOOR_DIV(DateTimeUtils.class, "floorDiv", long.class, long.class),
    FLOOR_MOD(DateTimeUtils.class, "floorMod", long.class, long.class),
    MODIFIABLE_TABLE_GET_MODIFIABLE_COLLECTION(ModifiableTable.class, "getModifiableCollection"),
    SCANNABLE_TABLE_SCAN(ScannableTable.class, "scan", DataContext.class),
    STRING_TO_DATE(DateTimeUtils.class, "dateStringToUnixDate", String.class),
    STRING_TO_TIME(DateTimeUtils.class, "timeStringToUnixDate", String.class),
    STRING_TO_TIMESTAMP(DateTimeUtils.class, "timestampStringToUnixDate", String.class),
    UNIX_DATE_TO_STRING(DateTimeUtils.class, "unixDateToString", int.class),
    UNIX_TIME_TO_STRING(DateTimeUtils.class, "unixTimeToString", int.class),
    UNIX_TIMESTAMP_TO_STRING(DateTimeUtils.class, "unixTimestampToString", long.class),
    INTERVAL_YEAR_MONTH_TO_STRING(DateTimeUtils.class, "intervalYearMonthToString", int.class, TimeUnitRange.class),
    INTERVAL_DAY_TIME_TO_STRING(
            DateTimeUtils.class,
            "intervalDayTimeToString",
            long.class,
            TimeUnitRange.class,
            int.class),
    UNIX_DATE_EXTRACT(DateTimeUtils.class, "unixDateExtract", TimeUnitRange.class, long.class),
    UNIX_DATE_FLOOR(DateTimeUtils.class, "unixDateFloor", TimeUnitRange.class, int.class),
    UNIX_DATE_CEIL(DateTimeUtils.class, "unixDateCeil", TimeUnitRange.class, int.class),
    UNIX_TIMESTAMP_FLOOR(DateTimeUtils.class, "unixTimestampFloor", TimeUnitRange.class, long.class),
    UNIX_TIMESTAMP_CEIL(DateTimeUtils.class, "unixTimestampCeil", TimeUnitRange.class, long.class),
    OBJECT_TO_STRING(Object.class, "toString"),
    OBJECTS_EQUAL(Objects.class, "equals", Object.class, Object.class),
    HASH(Utilities.class, "hash", int.class, Object.class),
    COMPARE(Utilities.class, "compare", Comparable.class, Comparable.class),
    COMPARE_NULLS_FIRST(Utilities.class, "compareNullsFirst", Comparable.class, Comparable.class),
    COMPARE_NULLS_LAST(Utilities.class, "compareNullsLast", Comparable.class, Comparable.class),
    IS_EMPTY(Collection.class, "isEmpty"),
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
    public final Constructor constructor;
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

    BuiltInMethod(Method method, Constructor constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /** Defines a method. */
    BuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /** Defines a constructor. */
    BuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /** Defines a field. */
    BuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }

    public String getMethodName() {
        return method.getName();
    }
}

// End BuiltInMethod.java
