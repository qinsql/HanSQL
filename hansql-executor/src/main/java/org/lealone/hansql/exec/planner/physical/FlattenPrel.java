/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.lealone.hansql.exec.planner.physical;

import org.lealone.hansql.common.expression.LogicalExpression;
import org.lealone.hansql.common.expression.SchemaPath;
import org.lealone.hansql.exec.physical.base.PhysicalOperator;
import org.lealone.hansql.exec.physical.config.FlattenPOP;
import org.lealone.hansql.exec.planner.logical.DrillOptiq;
import org.lealone.hansql.exec.planner.logical.DrillParseContext;
import org.lealone.hansql.exec.record.BatchSchema;
import org.lealone.hansql.optimizer.plan.RelOptCluster;
import org.lealone.hansql.optimizer.plan.RelTraitSet;
import org.lealone.hansql.optimizer.rel.RelNode;
import org.lealone.hansql.optimizer.rel.RelWriter;
import org.lealone.hansql.optimizer.rex.RexNode;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class FlattenPrel extends SinglePrel implements Prel {

  RexNode toFlatten;

  public FlattenPrel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode toFlatten) {
    super(cluster, traits, child);
    this.toFlatten = toFlatten;
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new FlattenPrel(getCluster(), traitSet, sole(inputs), toFlatten);
  }

  @Override
  public Iterator<Prel> iterator() {
    return PrelUtil.iter(getInput());
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    Prel child = (Prel) this.getInput();

    PhysicalOperator childPOP = child.getPhysicalOperator(creator);
    FlattenPOP f = new FlattenPOP(childPOP, (SchemaPath) getFlattenExpression(new DrillParseContext(PrelUtil.getSettings(getCluster()))));
    return creator.addMetadata(this, f);
  }

  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("flattenField", this.toFlatten);
  }

  @Override
  public BatchSchema.SelectionVectorMode getEncoding() {
    return BatchSchema.SelectionVectorMode.NONE;
  }

  protected LogicalExpression getFlattenExpression(DrillParseContext context){
    return DrillOptiq.toDrill(context, getInput(), toFlatten);
  }
}
