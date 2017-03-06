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

package io.crate.metadata.doc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreService;

import java.io.IOException;
import java.util.Map;

@Singleton
public class IndexMetaDataUpgradeService extends AbstractLifecycleComponent<IndexMetaDataUpgradeService>
    implements ClusterStateListener {

    private final ClusterService clusterService;
    private final RestoreService restoreService;
    private final RestoreSnapshotListener restoreSnapshotListener = new RestoreSnapshotListener();

    private boolean alreadyRun = false;

    @Inject
    public IndexMetaDataUpgradeService(Settings settings, ClusterService clusterService, RestoreService restoreService) {
        super(settings);
        this.clusterService = clusterService;
        this.restoreService = restoreService;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ClusterState state = event.state();
        if (!alreadyRun && state != null && event.localNodeMaster() && !state.metaData().indices().isEmpty()) {
            MetaData.Builder metaDataBuilder = MetaData.builder(state.metaData());
            boolean changed = true;
            for (IndexMetaData indexMetaData : state.metaData()) {
                try {
                    changed &= checkAndUpdateIndexMetaData(metaDataBuilder, indexMetaData);
                } catch (IOException e) {
                    logger.error("unable to read routing hash function type of index: {}",
                        indexMetaData.getIndex(), e);
                }
            }
            if (changed) {
                updateClusterState(metaDataBuilder);
            }
            alreadyRun = true;
        }
    }

    private static boolean checkAndUpdateIndexMetaData(MetaData.Builder metaDataBuilder,
                                                       IndexMetaData indexMetaData)  throws IOException {
        Map<String, Object> mappingMap = DocIndexMetaData.getMappingMap(indexMetaData);
        assert mappingMap != null : "Mapping metadata of index: " + indexMetaData.getIndex() + " is empty";
        String hashFunction = DocIndexMetaData.getRoutingHashFunctionType(mappingMap);
        if (hashFunction == null) {
            updateIndexRoutingHashFunction(metaDataBuilder, indexMetaData);
            return true;
        }
        return false;
    }

    private static void updateIndexRoutingHashFunction(MetaData.Builder metaDataBuilder,
                                                       IndexMetaData indexMetaData) throws IOException {
        String routingHashAlgorithm = indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION);
        if (routingHashAlgorithm == null) {
            routingHashAlgorithm = "Murmur3HashFunction";
        }
        Map<String, Object> mappingMap = DocIndexMetaData.getMappingMap(indexMetaData);
        mappingMap.put("routing_hash_algorithm", routingHashAlgorithm);
        metaDataBuilder.put(indexMetaData, true);
    }

    private void updateClusterState(MetaData.Builder metaDataBuilder) {
        clusterService.submitStateUpdateTask("state-upgrades-routing-hash-function-type", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                return ClusterState.builder(currentState)
                    .metaData(metaDataBuilder)
                    .build();
            }

            @Override
            public void onFailure(String source, Throwable t) {
                logger.error("unexpected failure during [{}]", t, source);
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                logger.info("upgraded routing hash algorithm meta data of [{}] indices",
                    newState.metaData().indices().size());
            }
        });
    }


    @Override
    protected void doStart() {
        clusterService.add(this);
        restoreService.addListener(restoreSnapshotListener);
    }

    @Override
    protected void doStop() {
        clusterService.remove(this);
        restoreService.removeListener(restoreSnapshotListener);
    }

    @Override
    protected void doClose() {
    }


    private class RestoreSnapshotListener implements ActionListener<RestoreService.RestoreCompletionResponse> {

        @Override
        public void onResponse(RestoreService.RestoreCompletionResponse restoreCompletionResponse) {
            MetaData.Builder metaDataBuilder = MetaData.builder(clusterService.state().metaData());
            boolean changed = true;
            for (String index : restoreCompletionResponse.getRestoreInfo().indices()) {
                IndexMetaData indexMetaData = clusterService.state().metaData().index(index);
                try {
                    changed &= checkAndUpdateIndexMetaData(metaDataBuilder, indexMetaData);
                } catch (IOException e) {
                    logger.error("unable to read routing hash function type of restored index: {}",
                        indexMetaData.getIndex(), e);
                }
            }
            if (changed) {
                updateClusterState(metaDataBuilder);
            }
        }

        @Override
        public void onFailure(Throwable e) {
        }
    }

}
