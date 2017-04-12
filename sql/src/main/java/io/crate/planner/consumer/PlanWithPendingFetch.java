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

import io.crate.planner.Plan;
import io.crate.planner.PlanVisitor;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.fetch.FetchPushDown;
import io.crate.planner.projection.Projection;

import javax.annotation.Nullable;
import java.util.UUID;

public class PlanWithPendingFetch implements Plan {

    private final Plan innerPlan;
    private final FetchPushDown.PhaseAndProjection fetchPhaseAndProjection;

    public PlanWithPendingFetch(Plan innerPlan, FetchPushDown.PhaseAndProjection fetchPhaseAndProjection) {
        this.innerPlan = innerPlan;
        this.fetchPhaseAndProjection = fetchPhaseAndProjection;
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context) {
        return visitor.process(innerPlan, context);
    }

    @Override
    public UUID jobId() {
        return innerPlan.jobId();
    }

    @Override
    public void addProjection(Projection projection, @Nullable Integer newLimit, @Nullable Integer newOffset, @Nullable Integer newNumOutputs, @Nullable PositionalOrderBy newOrderBy) {
        innerPlan.addProjection(projection, newLimit, newOffset, newNumOutputs, newOrderBy);
    }

    @Override
    public ResultDescription resultDescription() {
        return innerPlan.resultDescription();
    }

    @Override
    public void setDistributionInfo(DistributionInfo distributionInfo) {
        innerPlan.setDistributionInfo(distributionInfo);
    }

    public FetchPushDown.PhaseAndProjection fetchPhaseAndProjection() {
        return fetchPhaseAndProjection;
    }
}
