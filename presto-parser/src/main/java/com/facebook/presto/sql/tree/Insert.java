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
package com.facebook.presto.sql.tree;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Insert
        extends Statement
{
    private final QualifiedName target;
    private final Query query;
    private final Optional<List<String>> columns;
    private final boolean triggeredByRefresh;

    public Insert(QualifiedName target, Optional<List<String>> columns, Query query)
    {
        this(Optional.empty(), columns, target, query, false);
    }

    public Insert(QualifiedName target, Optional<List<String>> columns, Query query, boolean triggeredByRefresh)
    {
        this(Optional.empty(), columns, target, query, triggeredByRefresh);
    }

    private Insert(Optional<NodeLocation> location, Optional<List<String>> columns, QualifiedName target, Query query, boolean triggeredByRefresh)
    {
        super(location);
        this.target = requireNonNull(target, "target is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.query = requireNonNull(query, "query is null");
        this.triggeredByRefresh = triggeredByRefresh;
    }

    public QualifiedName getTarget()
    {
        return target;
    }

    public Optional<List<String>> getColumns()
    {
        return columns;
    }

    public Query getQuery()
    {
        return query;
    }

    public boolean isTriggeredByRefresh()
    {
        return triggeredByRefresh;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitInsert(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, columns, query, triggeredByRefresh);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        Insert o = (Insert) obj;
        return Objects.equals(target, o.target) &&
                Objects.equals(columns, o.columns) &&
                Objects.equals(query, o.query) &&
                Objects.equals(triggeredByRefresh, o.triggeredByRefresh);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("target", target)
                .add("columns", columns)
                .add("query", query)
                .add("triggeredByRefresh", triggeredByRefresh)
                .toString();
    }
}
