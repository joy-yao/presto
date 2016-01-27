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
package com.facebook.presto.spi;

import com.sun.xml.internal.fastinfoset.QualifiedName;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MaterializedQueryTableInfo
{
    private final String query;
    private final Map<QualifiedName, ConnectorTableHandle> baseTables;
    private final Map<QualifiedName, ColumnHandle> baseTableColumns;

    public MaterializedQueryTableInfo(String query, Map<QualifiedName, ConnectorTableHandle> baseTables, Map<QualifiedName, ColumnHandle> baseTableColumns)
    {
        this.query = requireNonNull(query, "query is null");
        this.baseTables = requireNonNull(baseTables, "baseTables is null");
        this.baseTableColumns = requireNonNull(baseTableColumns, "baseTableColumns is null");
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MaterializedQueryTableInfo that = (MaterializedQueryTableInfo) o;
        return Objects.equals(query, that.query) &&
                Objects.equals(baseTables, that.baseTables) &&
                Objects.equals(baseTableColumns, that.baseTableColumns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, baseTables, baseTableColumns);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("MaterializedQueryTableInfo{");
        sb.append("query=").append(query);
        sb.append(", baseTables=").append(baseTables);
        sb.append(", baseTableColumns=").append(baseTableColumns);
        sb.append('}');
        return sb.toString();
    }
}
