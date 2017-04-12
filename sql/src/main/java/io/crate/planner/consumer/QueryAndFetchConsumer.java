/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner.consumer;

import com.google.common.collect.ImmutableList;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.collections.Lists2;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.exceptions.VersionInvalidException;
import io.crate.operation.predicate.MatchPredicate;
import io.crate.operation.projectors.TopN;
import io.crate.planner.*;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.OrderedTopNProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class QueryAndFetchConsumer implements Consumer {

    private final Visitor visitor;

    public QueryAndFetchConsumer() {
        visitor = new Visitor();
    }

    @Override
    public Plan consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private static final NoPredicateVisitor NO_PREDICATE_VISITOR = new NoPredicateVisitor();

        @Override
        public Plan visitQueriedDocTable(QueriedDocTable table, ConsumerContext context) {
            if (isNoSimpleSelect(table, context)) return null;

            if (context.fetchRewriteDisabled()) {
                return normalSelect(table, context);
            }

            FetchPushDown.Builder<QueriedDocTable> fetchPhaseBuilder = FetchPushDown.pushDown(table);
            if (fetchPhaseBuilder == null) {
                return normalSelect(table, context);
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan plan = normalSelect(fetchPhaseBuilder.replacedRelation(), context);
            return new PlanWithPendingFetch(plan, fetchPhaseBuilder.build(plannerContext));
        }

        @Override
        public Plan visitQueriedTable(QueriedTable table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            if (querySpec.hasAggregates()) {
                return null;
            }
            if (querySpec.where().hasQuery()) {
                ensureNoLuceneOnlyPredicates(querySpec.where().query());
            }
            return normalSelect(table, context);
        }

        @Override
        public Plan visitQueriedSelectRelation(QueriedSelectRelation relation, ConsumerContext context) {
            QuerySpec qs = relation.querySpec();
            if (qs.hasAggregates() || qs.groupBy().isPresent()) {
                return null;
            }
            Planner.Context plannerContext = context.plannerContext();
            Plan subPlan = plannerContext.planSubRelation(relation.subRelation(), context);
            Limits limits = plannerContext.getLimits(qs);

            OrderBy orderBy = qs.orderBy().orElse(null);
            if (subPlan instanceof PlanWithPendingFetch) {
                PlanWithPendingFetch planWithPendingFetch = (PlanWithPendingFetch) subPlan;
                Plan innerPlan = planWithPendingFetch.innerPlan();

                if (orderBy == null) {
                    // FIXME: If orderBy is null we could have rewritten the query - assert here that orderBy is not null and remove
                    // this code?
                    if (limits.finalLimit() == TopN.NO_LIMIT && limits.offset() == 0) {
                        return Merge.applyFetch(planWithPendingFetch, plannerContext);
                    }
                    innerPlan = Merge.ensureOnHandler(innerPlan, plannerContext);
                    TopNProjection topN = new TopNProjection(
                        limits.finalLimit(),
                        limits.offset(),
                        InputColumn.fromTypes(innerPlan.resultDescription().streamOutputs()));
                    innerPlan.addProjection(topN, null, null, null, null);
                } else {
                    innerPlan = Merge.ensureOnHandler(innerPlan, plannerContext);
                    OrderedTopNProjection orderedTopNProjection = new OrderedTopNProjection(
                        limits.finalLimit(),
                        limits.offset(),
                        InputColumn.fromTypes(innerPlan.resultDescription().streamOutputs()),
                        Collections.singletonList(new InputColumn(0, null)), // FIXME
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst()
                    );

                    innerPlan.addProjection(orderedTopNProjection, null, null, null, null);
                    FetchPushDown.PhaseAndProjection phaseAndProjection = planWithPendingFetch.phaseAndProjection();
                    innerPlan.addProjection(
                        phaseAndProjection.projection, null, null, phaseAndProjection.projection.outputs().size(), null);
                    return new QueryThenFetch(innerPlan, phaseAndProjection.phase);
                }
            } else {
                subPlan = Merge.ensureOnHandler(subPlan, plannerContext);
                Projection topN = ProjectionBuilder.topNOrEval(
                    relation.subRelation().fields(),
                    orderBy,
                    limits.offset(),
                    limits.finalLimit(),
                    qs.outputs()
                );
                subPlan.addProjection(topN, null, null, qs.outputs().size(), null);
            }

            return subPlan;
        }

        private void ensureNoLuceneOnlyPredicates(Symbol query) {
            NO_PREDICATE_VISITOR.process(query, null);
        }

        private static class NoPredicateVisitor extends SymbolVisitor<Void, Void> {
            @Override
            public Void visitFunction(Function symbol, Void context) {
                if (symbol.info().ident().name().equals(MatchPredicate.NAME)) {
                    throw new UnsupportedFeatureException("Cannot use match predicate on system tables");
                }
                for (Symbol argument : symbol.arguments()) {
                    process(argument, context);
                }
                return null;
            }
        }

        private static Plan normalSelect(QueriedTableRelation table, ConsumerContext context) {
            QuerySpec querySpec = table.querySpec();
            Planner.Context plannerContext = context.plannerContext();
            /*
             * ORDER BY columns are added to OUTPUTS - they're required to do an ordered merge.
             * select id, name, order by id, date
             *
             * toCollect:           [id, name, date]       // includes order by symbols, that aren't already selected
             * outputsInclOrder:    [in(0), in(1), in(2)]  // for topN projection on shards/collectPhase
             * orderByInputs:       [in(0), in(2)]         // for topN projection on shards/collectPhase AND handler
             * finalOutputs:        [in(0), in(1)]         // for topN output on handler -> changes output to what should be returned.
             */
            List<Symbol> qsOutputs = querySpec.outputs();
            List<Symbol> toCollect;
            Optional<OrderBy> optOrderBy = querySpec.orderBy();
            table.tableRelation().validateOrderBy(optOrderBy);
            if (optOrderBy.isPresent()) {
                toCollect = Lists2.concatUnique(qsOutputs, optOrderBy.get().orderBySymbols());
            } else {
                toCollect = qsOutputs;
            }
            List<Symbol> outputsInclOrder = InputColumn.fromSymbols(toCollect);

            Limits limits = plannerContext.getLimits(querySpec);
            List<Projection> projections = ImmutableList.of();
            if (limits.hasLimit()) {
                TopNProjection collectTopN = new TopNProjection(limits.limitAndOffset(), 0, outputsInclOrder);
                projections = Collections.singletonList(collectTopN);
            }
            RoutedCollectPhase collectPhase = RoutedCollectPhase.forQueriedTable(
                plannerContext,
                table,
                toCollect,
                projections
            );
            Integer requiredPageSize = context.requiredPageSize();
            if (requiredPageSize == null) {
                if (limits.hasLimit()) {
                    collectPhase.nodePageSizeHint(limits.limitAndOffset());
                }
            } else {
                collectPhase.pageSizeHint(requiredPageSize);
            }
            collectPhase.orderBy(optOrderBy.orElse(null));

            return new Collect(
                collectPhase,
                limits.finalLimit(),
                limits.offset(),
                qsOutputs.size(),
                limits.limitAndOffset(),
                PositionalOrderBy.of(optOrderBy.orElse(null), toCollect)
            );
        }
    }

    private static boolean isNoSimpleSelect(QueriedDocTable table, ConsumerContext context) {
        if (table.querySpec().hasAggregates() || table.querySpec().groupBy().isPresent()) {
            return true;
        }
        if (table.querySpec().where().hasVersions()) {
            context.validationException(new VersionInvalidException());
            return true;
        }
        return false;
    }
}
