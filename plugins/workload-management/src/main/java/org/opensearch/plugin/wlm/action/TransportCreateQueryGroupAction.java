/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.wlm.service.QueryGroupPersistenceService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.util.concurrent.ExecutorService;

/**
 * Transport action to create QueryGroup
 *
 * @opensearch.experimental
 */
public class TransportCreateQueryGroupAction extends HandledTransportAction<CreateQueryGroupRequest, CreateQueryGroupResponse> {

    private final ThreadPool threadPool;
    private final QueryGroupPersistenceService queryGroupPersistenceService;
    private final Settings settings;

    /**
     * Constructor for TransportCreateQueryGroupAction
     *
     * @param actionName - action name
     * @param transportService - a {@link TransportService} object
     * @param actionFilters - a {@link ActionFilters} object
     * @param threadPool - a {@link ThreadPool} object
     * @param queryGroupPersistenceService - a {@link QueryGroupPersistenceService} object
     */
    @Inject
    public TransportCreateQueryGroupAction(
        String actionName,
        TransportService transportService,
        ActionFilters actionFilters,
        ThreadPool threadPool,
        QueryGroupPersistenceService queryGroupPersistenceService
    ) {
        super(CreateQueryGroupAction.NAME, transportService, actionFilters, CreateQueryGroupRequest::new);
        this.threadPool = threadPool;
        this.queryGroupPersistenceService = queryGroupPersistenceService;
        this.settings = this.queryGroupPersistenceService.getSettings();
    }

//    @Override
//    protected void doExecute(Task task, CreateQueryGroupRequest request, ActionListener<CreateQueryGroupResponse> listener) {
//        threadPool.executor(ThreadPool.Names.SAME)
//            .execute(() -> queryGroupPersistenceService.persistInClusterStateMetadata(request.getQueryGroup(), listener));
//    }

    @Override
    protected void doExecute(Task task, CreateQueryGroupRequest request, ActionListener<CreateQueryGroupResponse> listener) {
        String queryGroupId = request.getQueryName();

        try {
            int maxThreads = 10;
            int queueSize = 1000;

            // Create and register the thread pool for the new QueryGroup
            threadPool.createAndRegisterThreadPoolForQueryGroup(
                settings,
                queryGroupId,
                maxThreads,
                queueSize
            );
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        // for testing
//        ExecutorService executorService = threadPool.executorForQueryGroup(queryGroupId);
//        executorService.execute(() -> System.out.println("Task executed in thread pool for QueryGroup: " + queryGroupId));


        threadPool.executor(ThreadPool.Names.SAME)
            .execute(() -> queryGroupPersistenceService.persistInClusterStateMetadata(request.getQueryGroup(), listener));
    }
}
