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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class MaterializedQueryTableInfo
{
    private final String query;
    private final Map<String, byte[]> baseTables;
    private final Map<String, byte[]> baseTableColumns;

    @JsonCreator
    public MaterializedQueryTableInfo(
            @JsonProperty("query") String query,
            @JsonProperty("baseTables") Map<String, byte[]> baseTables,
            @JsonProperty("baseTableColumns") Map<String, byte[]> baseTableColumns)
    {
        this.query = requireNonNull(query, "query is null");
        this.baseTables = requireNonNull(baseTables, "baseTables is null");
        this.baseTableColumns = requireNonNull(baseTableColumns, "baseTableColumns is null");
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Map<String, byte[]> getBaseTables()
    {
        return baseTables;
    }

    @JsonProperty
    public Map<String, byte[]> getBaseTableColumns()
    {
        return baseTableColumns;
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
//
//    @Override
//    public String toString()
//    {
//        return toStringHelper(this)
//                .add("session", session)
//                .add("fragment", fragment)
//                .add("sources", sources)
//                .add("outputIds", outputIds)
//                .toString();
//    }
}
