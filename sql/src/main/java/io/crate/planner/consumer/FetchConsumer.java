/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.consumer;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.planner.Merge;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.dql.QueryThenFetch;

public final class FetchConsumer implements Consumer {

    private final Visitor visitor;

    FetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            return createQueriedDocTablePlan(table, context);
        }

        @Override
        public Plan visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, ConsumerContext context) {
            return createMSSPlan(multiSourceSelect, context);
        }
    }

    private static Plan createQueriedDocTablePlan(QueriedDocTable table, ConsumerContext context) {
        QuerySpec qs = table.querySpec();
        if (context.fetchRewriteDisabled() || fetchIsNotApplicable(qs)) {
            return null;
        }
        FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(table);
        if (fetchPhaseBuilder == null) {
            return null;
        }
        AnalyzedRelation subRelation = fetchPhaseBuilder.replacedRelation();
        Planner.Context plannerContext = context.plannerContext();
        context.disableFetchRewrite(); // avoid fetch-consumer recursion
        Plan plannedSubQuery = Merge.ensureOnHandler(
            plannerContext.planSubRelation(subRelation, context),
            plannerContext
        );

        // fetch phase and projection can only be build after the sub-plan was processed (shards/readers allocated)
        FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(plannerContext);
        plannedSubQuery.addProjection(
            fetchPhaseAndProjection.projection,
            null,
            null,
            fetchPhaseAndProjection.projection.outputs().size(),
            null);

        return new QueryThenFetch(plannedSubQuery, fetchPhaseAndProjection.phase);
    }

    private static Plan createMSSPlan(MultiSourceSelect mss, ConsumerContext context) {
        if (context.fetchRewriteDisabled() || fetchIsNotApplicable(mss.querySpec()) || mss.canBeFetched().isEmpty()) {
            /* TODO: remove this
             * currently without this statements like  select min(u1.name) from users u1, users u2
             * result in a plan with with query-then-fetch:
             *
             * QTF:
             *  subPlan: NestedLoop
             *              projections:
             *                  fetch,
             *                  aggregation,
             */
            context.disableFetchRewrite();
            return null;
        }
        context.disableFetchRewrite();
        FetchPushDown.Builder fetchPhaseBuilder = FetchPushDown.pushDown(mss);
        assert fetchPhaseBuilder != null : "expecting fetchPhaseBuilder not to be null";

        Planner.Context plannerContext = context.plannerContext();
        Plan plannedSubQuery = Merge.ensureOnHandler(
            plannerContext.planSubRelation(mss, context),
            plannerContext
        );
        assert plannedSubQuery != null : "consumingPlanner should have created a subPlan";
        FetchPushDown.PhaseAndProjection fetchPhaseAndProjection = fetchPhaseBuilder.build(plannerContext);
        plannedSubQuery.addProjection(
            fetchPhaseAndProjection.projection,
            null,
            null,
            fetchPhaseAndProjection.projection.outputs().size(),
            null);
        return new QueryThenFetch(plannedSubQuery, fetchPhaseAndProjection.phase);
    }

    private static boolean fetchIsNotApplicable(QuerySpec qs) {
        return qs.hasAggregates() || qs.groupBy().isPresent();
    }
}
