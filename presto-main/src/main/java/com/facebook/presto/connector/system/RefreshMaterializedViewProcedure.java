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
package com.facebook.presto.connector.system;

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.procedure.Procedure;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.util.Objects.requireNonNull;

public class RefreshMaterializedViewProcedure
{
    private final QueryManager queryManager;

    @Inject
    public RefreshMaterializedViewProcedure(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
    }

    @UsedByGeneratedCode
    public void refresh(String materializedView, ConnectorSession session)
    {
        try {
            queryManager.refreshMaterializedView(materializedView, session);
        }
        catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public Procedure getProcedure()
    {
        return new Procedure(
                "runtime",
                "refresh_materialized_view",
                ImmutableList.of(new Procedure.Argument("materializedView", VARCHAR)),
                methodHandle(getClass(), "refresh", String.class, ConnectorSession.class).bindTo(this));
    }
}
