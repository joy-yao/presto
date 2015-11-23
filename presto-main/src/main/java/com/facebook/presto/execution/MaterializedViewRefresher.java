/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeClientSupplier;
import com.facebook.presto.server.StatementResource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class MaterializedViewRefresher
        implements MaterializedViewRefresherInterface
{
    private final QueryManager queryManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final QueryIdGenerator queryIdGenerator;
    private final Metadata metadata;

    @Inject
    public MaterializedViewRefresher(
            QueryManager queryManager,
            Metadata metadata,
            ExchangeClientSupplier exchangeClientSupplier,
            QueryIdGenerator queryIdGenerator)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
    }

    @Override
    public void refreshMaterializedView(String materializedView, ConnectorSession connectorSession)
            throws Exception
    {
        requireNonNull(materializedView, "materializedView is null");
        checkArgument(!materializedView.trim().isEmpty(), "materializedView must not be empty string");

        Session session = Session.builder(metadata.getSessionPropertyManager())
                .setQueryId(new QueryId(connectorSession.getQueryId()))
                .setIdentity(connectorSession.getIdentity())
                .setSource("system")
                .setCatalog("") // default catalog is not be used
                .setSchema("") // default schema is not be used
                .setTimeZoneKey(connectorSession.getTimeZoneKey())
                .setLocale(connectorSession.getLocale())
                .setStartTime(connectorSession.getStartTime())
                .build();

        QueryId deleteQueryId = queryIdGenerator.createNextQueryId();
        QueryInfo queryInfo = queryManager.createQuery(session, "DELETE FROM " + materializedView, deleteQueryId);
        queryInfo = waitForQueryToFinish(queryInfo, deleteQueryId, session);
        if (queryInfo.getState() != QueryState.FINISHED) {
            throw new Exception("Delete from materialized view failed");
        }
        queryInfo = queryManager.createQuery(session, "INSERT INTO " + materializedView + " VALUES (1)");
        queryInfo = waitForQueryToFinish(queryInfo, session.getQueryId(), session);
        if (queryInfo.getState() != QueryState.FINISHED) {
            throw new Exception("Insert into materialized view failed");
        }
    }

    private QueryInfo waitForQueryToFinish(QueryInfo queryInfo, QueryId queryId, Session session)
            throws InterruptedException
    {
        ExchangeClient exchangeClient = exchangeClientSupplier.get(deltaMemoryInBytes -> {
        });
        // wait for it to start
        while (!isQueryStarted(queryInfo)) {
            queryManager.recordHeartbeat(queryId);
            queryManager.waitForStateChange(queryId, queryInfo.getState(), new Duration(1, TimeUnit.SECONDS));
            queryInfo = queryManager.getQueryInfo(queryId);
        }

        while (!queryInfo.getState().isDone()) {
            queryManager.recordHeartbeat(queryId);
            Iterable<List<Object>> data = getData(queryInfo, exchangeClient, session); // ignore data -- we don't need it for delete or insert.
            queryManager.waitForStateChange(queryId, queryInfo.getState(), new Duration(1, TimeUnit.SECONDS));
            queryInfo = queryManager.getQueryInfo(queryId);
        }
        return queryInfo;
    }

    private static boolean isQueryStarted(QueryInfo queryInfo)
    {
        QueryState state = queryInfo.getState();
        return state != QueryState.QUEUED && queryInfo.getState() != QueryState.PLANNING && queryInfo.getState() != QueryState.STARTING;
    }

    private Iterable<List<Object>> getData(QueryInfo queryInfo, ExchangeClient exchangeClient, Session session)
            throws InterruptedException
    {
        List<Type> types = queryInfo.getOutputStage().getTypes();

        updateExchangeClient(queryInfo.getOutputStage(), exchangeClient);

        ImmutableList.Builder<StatementResource.Query.RowIterable> pages = ImmutableList.builder();
        // wait up to max wait for data to arrive; then try to return at least DESIRED_RESULT_BYTES
        long bytes = 0;
        while (bytes < DESIRED_RESULT_BYTES) {
            Page page = exchangeClient.getNextPage(new Duration(10, TimeUnit.MILLISECONDS));
            if (page == null) {
                break;
            }
            bytes += page.getSizeInBytes();
            pages.add(new StatementResource.Query.RowIterable(session.toConnectorSession(), types, page));
        }

        if (bytes == 0) {
            return null;
        }

        return Iterables.concat(pages.build());
    }

    private static final long DESIRED_RESULT_BYTES = new DataSize(1, DataSize.Unit.MEGABYTE).toBytes();

    private synchronized void updateExchangeClient(StageInfo outputStage, ExchangeClient exchangeClient)
    {
        // add any additional output locations
        if (!outputStage.getState().isDone()) {
            for (TaskInfo taskInfo : outputStage.getTasks()) {
                SharedBufferInfo outputBuffers = taskInfo.getOutputBuffers();
                List<BufferInfo> buffers = outputBuffers.getBuffers();
                if (buffers.isEmpty() || outputBuffers.getState().canAddBuffers()) {
                    // output buffer has not been created yet
                    continue;
                }
                Preconditions.checkState(buffers.size() == 1,
                        "Expected a single output buffer for task %s, but found %s",
                        taskInfo.getTaskId(),
                        buffers);

                TaskId bufferId = Iterables.getOnlyElement(buffers).getBufferId();
                URI uri = uriBuilderFrom(taskInfo.getSelf()).appendPath("results").appendPath(bufferId.toString()).build();
                exchangeClient.addLocation(uri);
            }
        }

        if (allOutputBuffersCreated(outputStage)) {
            exchangeClient.noMoreLocations();
        }
    }

    private static boolean allOutputBuffersCreated(StageInfo outputStage)
    {
        StageState stageState = outputStage.getState();

        // if the stage is already done, then there will be no more buffers
        if (stageState.isDone()) {
            return true;
        }

        // have all stage tasks been scheduled?
        if (stageState == StageState.PLANNED || stageState == StageState.SCHEDULING) {
            return false;
        }

        // have all tasks finished adding buffers
        return outputStage.getTasks().stream()
                .allMatch(taskInfo -> !taskInfo.getOutputBuffers().getState().canAddBuffers());
    }
}
