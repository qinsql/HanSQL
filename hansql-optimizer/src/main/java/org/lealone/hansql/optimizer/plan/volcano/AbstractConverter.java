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
package org.lealone.hansql.optimizer.plan.volcano;

import java.util.List;

import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelOptCost;
import org.lealone.hansql.optimizer.plan.RelOptPlanner;
import org.lealone.hansql.optimizer.plan.RelOptRule;
import org.lealone.hansql.optimizer.plan.RelOptRuleCall;
import org.lealone.hansql.optimizer.plan.RelTrait;
import org.lealone.hansql.optimizer.plan.RelTraitDef;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelWriter;
import org.lealone.hansql.optimizer.rel.convert.ConverterImpl;
import org.lealone.hansql.optimizer.rel.core.RelFactories;
import org.lealone.hansql.optimizer.rel.metadata.RelMetadataQuery;
import org.lealone.hansql.optimizer.tools.RelBuilderFactory;

/**
 * Converts a relational expression to any given output convention.
 *
 * <p>Unlike most {@link org.lealone.hansql.optimizer.rel.convert.Converter}s, an abstract
 * converter is always abstract. You would typically create an
 * <code>AbstractConverter</code> when it is necessary to transform a relational
 * expression immediately; later, rules will transform it into relational
 * expressions which can be implemented.
 *
 * <p>If an abstract converter cannot be satisfied immediately (because the
 * source subset is abstract), the set is flagged, so this converter will be
 * expanded as soon as a non-abstract relexp is added to the set.</p>
 */
public class AbstractConverter extends ConverterImpl {
  //~ Constructors -----------------------------------------------------------

  public AbstractConverter(
      RelOptCluster cluster,
      RelSubset rel,
      RelTraitDef traitDef,
      RelTraitSet traits) {
    super(cluster, traitDef, traits, rel);
    assert traits.allSimple();
  }

  //~ Methods ----------------------------------------------------------------


  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new AbstractConverter(
        getCluster(),
        (RelSubset) sole(inputs),
        traitDef,
        traitSet);
  }

  public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
    return planner.getCostFactory().makeInfiniteCost();
  }

  public RelWriter explainTerms(RelWriter pw) {
    super.explainTerms(pw);
    for (RelTrait trait : traitSet) {
      pw.item(trait.getTraitDef().getSimpleName(), trait);
    }
    return pw;
  }

  //~ Inner Classes ----------------------------------------------------------

  /**
   * Rule which converts an {@link AbstractConverter} into a chain of
   * converters from the source relation to the target traits.
   *
   * <p>The chain produced is minimal: we have previously built the transitive
   * closure of the graph of conversions, so we choose the shortest chain.</p>
   *
   * <p>Unlike the {@link AbstractConverter} they are replacing, these
   * converters are guaranteed to be able to convert any relation of their
   * calling convention. Furthermore, because they introduce subsets of other
   * calling conventions along the way, these subsets may spawn more efficient
   * conversions which are not generally applicable.</p>
   *
   * <p>AbstractConverters can be messy, so they restrain themselves: they
   * don't fire if the target subset already has an implementation (with less
   * than infinite cost).</p>
   */
  public static class ExpandConversionRule extends RelOptRule {
    public static final ExpandConversionRule INSTANCE =
        new ExpandConversionRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates an ExpandConversionRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public ExpandConversionRule(RelBuilderFactory relBuilderFactory) {
      super(operand(AbstractConverter.class, any()), relBuilderFactory, null);
    }

    public void onMatch(RelOptRuleCall call) {
      final VolcanoPlanner planner = (VolcanoPlanner) call.getPlanner();
      AbstractConverter converter = call.rel(0);
      final RelNode child = converter.getInput();
      RelNode converted =
          planner.changeTraitsUsingConverters(
              child,
              converter.traitSet);
      if (converted != null) {
        call.transformTo(converted);
      }
    }
  }
}

// End AbstractConverter.java
