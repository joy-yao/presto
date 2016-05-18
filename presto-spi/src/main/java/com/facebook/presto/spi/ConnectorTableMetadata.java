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

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class ConnectorTableMetadata
{
    private final SchemaTableName table;
    private final List<ColumnMetadata> columns;
    private final Map<String, Object> properties;
    /* nullable */
    private final String owner;
    private final boolean sampled;
    private final Optional<MaterializedQueryTableInfo> materializedQueryTableInfo;

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns)
    {
        this(table, columns, emptyMap(), null);
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties)
    {
        this(table, columns, properties, null);
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties, String owner)
    {
        this(table, columns, properties, owner, false);
    }

    public ConnectorTableMetadata(SchemaTableName table, List<ColumnMetadata> columns, Map<String, Object> properties, String owner, boolean sampled)
    {
        this(table, columns, properties, owner, sampled, Optional.empty());
    }

    public ConnectorTableMetadata(
            SchemaTableName table,
            List<ColumnMetadata> columns,
            Map<String, Object> properties,
            String owner,
            boolean sampled,
            Optional<MaterializedQueryTableInfo> materializedQueryTableInfo)
    {
        if (table == null) {
            throw new NullPointerException("table is null or empty");
        }
        if (columns == null) {
            throw new NullPointerException("columns is null");
        }

        this.table = table;
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        this.owner = owner;
        this.sampled = sampled;
        this.materializedQueryTableInfo = requireNonNull(materializedQueryTableInfo, "materializedQueryTableInfo is null");
    }

    public boolean isSampled()
    {
        return sampled;
    }

    public Optional<MaterializedQueryTableInfo> getMaterializedQueryTableInfo()
    {
        return materializedQueryTableInfo;
    }

    public SchemaTableName getTable()
    {
        return table;
    }

    public List<ColumnMetadata> getColumns()
    {
        return columns;
    }

    public Map<String, Object> getProperties()
    {
        return properties;
    }

    /**
     * @return table owner or null
     */
    public String getOwner()
    {
        return owner;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ConnectorTableMetadata{");
        sb.append("table=").append(table);
        sb.append(", columns=").append(columns);
        sb.append(", properties=").append(properties);
        sb.append(", owner=").append(owner);
        sb.append(", materializedQueryTableInfo=").append(materializedQueryTableInfo);
        sb.append('}');
        return sb.toString();
    }
}
