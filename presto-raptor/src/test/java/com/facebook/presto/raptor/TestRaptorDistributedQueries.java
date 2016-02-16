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
package com.facebook.presto.raptor;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorQueryRunner.createRaptorQueryRunner;
import static com.facebook.presto.raptor.RaptorQueryRunner.createSampledSession;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

public class TestRaptorDistributedQueries
        extends AbstractTestDistributedQueries
{
    @SuppressWarnings("unused")
    public TestRaptorDistributedQueries()
            throws Exception
    {
        this(createRaptorQueryRunner(ImmutableMap.of(), true, false));
    }

    protected TestRaptorDistributedQueries(QueryRunner queryRunner)
    {
        super(queryRunner, createSampledSession());
    }

    @Test
    public void testCreateArrayTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE array_test AS SELECT ARRAY [1, 2, 3] AS c", 1);
        assertQuery("SELECT cardinality(c) FROM array_test", "SELECT 3");
        assertUpdate("DROP TABLE array_test");
    }

    @Test
    public void testMapTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE map_test AS SELECT MAP(ARRAY [1, 2, 3], ARRAY ['hi', 'bye', NULL]) AS c", 1);
        assertQuery("SELECT c[1] FROM map_test", "SELECT 'hi'");
        assertQuery("SELECT c[3] FROM map_test", "SELECT NULL");
        assertUpdate("DROP TABLE map_test");
    }

    @Test
    public void testRefreshMaterializedQueryTable()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_refresh_base AS " +
                "SELECT a, b, c FROM (VALUES (1, 2, 3), (1, 2, 4), (2, 3, 4), (3, 4, 5), (10, 20, 30)) t(a, b, c)", 4);

        // Create two materialized query tables. One without filtering and one with.
        assertUpdate("CREATE TABLE test_refresh_mqt1 AS " +
                "SELECT a, b, SUM(c) as c from raptor.tpch.test_refresh_base " +
                "GROUP BY a, b " +
                "WITH NO DATA " +
                "REFRESH ON DEMAND", 4);
        assertUpdate("CREATE TABLE test_refresh_mqt2 AS " +
                "SELECT a, b, SUM(c) as c from test_refresh_base " +
                "WHERE b < 10 " +
                "GROUP BY a, b " +
                "WITH NO DATA " +
                "REFRESH ON DEMAND", 3);

        // Refresh both materialized tables with no predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt1', '', '')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt2', '', '')");

        MaterializedResult materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows1 = computeActual("SELECT a, b, c FROM test_refresh_mqt1 ORDER BY a, b");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 5L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), 10L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), 20L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 30L);

        MaterializedResult materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 3L);

        materializedRows2 = computeActual("SELECT a, b, c FROM test_refresh_mqt2 ORDER BY a, b");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 5L);

        queryRunner.execute(getSession(), "INSERT INTO test_refresh_base values (1, 2, 10), (2, 3, 5), (10, 20, 5)");

        // Refresh both materialized tables with predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt1', '{\"raptor.tpch.test_refresh_base\":\"a > 1\"}', 'a > 1')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table('raptor.tpch.test_refresh_mqt2', '{\"test_refresh_base\":\"a > 1\"}', 'a > 1')");

        materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows1 = computeActual("SELECT a, b, c FROM test_refresh_mqt1 ORDER BY a, b");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 9L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 5L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), 10L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), 20L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 35L);

        materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 3L);

        materializedRows2 = computeActual("SELECT a, b, c FROM test_refresh_mqt2 ORDER BY a, b");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 1L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 7L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), 2L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 9L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), 3L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), 4L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 5L);
    }

    @Test
    public void testRefreshMaterializedQueryTableWithJoin()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_mqt_fact AS " +
                "SELECT id, cust_id, ds, value " +
                "FROM (VALUES (0, 100, '2015-02-01', 10), (1, 100, '2015-02-01', 11), (2, 200, '2015-02-01', 12), (3, 100, '2015-02-02', 13), (4, 300, '2015-02-02', 14)) t(id, cust_id, ds, value)", 4);

        assertUpdate("CREATE TABLE test_mqt_cust AS " +
                "SELECT cust_id, name " +
                "FROM (VALUES (100, 'Customer1'), (200, 'Customer2'), (300, 'Customer3')) t(cust_id, name)", 2);

        // Create two materialized query tables. One without filtering and one with.
        assertUpdate("CREATE TABLE test_refresh_mqt_join_1 AS " +
                "SELECT raptor.tpch.test_mqt_cust.name, raptor.tpch.test_mqt_fact.ds, SUM(raptor.tpch.test_mqt_fact.value) as sum " +
                "FROM raptor.tpch.test_mqt_fact INNER JOIN raptor.tpch.test_mqt_cust ON raptor.tpch.test_mqt_fact.cust_id = raptor.tpch.test_mqt_cust.cust_id " +
                "GROUP BY test_mqt_cust.name, test_mqt_fact.ds " +
                "WITH DATA " +
                "REFRESH ON DEMAND", 5);
        assertUpdate("CREATE TABLE test_refresh_mqt_join_2 AS " +
                "SELECT raptor.tpch.test_mqt_cust.name, raptor.tpch.test_mqt_fact.ds, SUM(raptor.tpch.test_mqt_fact.value) as sum " +
                "FROM raptor.tpch.test_mqt_fact INNER JOIN raptor.tpch.test_mqt_cust ON raptor.tpch.test_mqt_fact.cust_id = raptor.tpch.test_mqt_cust.cust_id " +
                "WHERE raptor.tpch.test_mqt_fact.cust_id < 300 " +
                "GROUP BY test_mqt_cust.name, test_mqt_fact.ds " +
                "WITH DATA " +
                "REFRESH ON DEMAND", 5);

        MaterializedResult materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows1 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_1 ORDER BY name, ds");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 13L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 12L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), "Customer3");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 14L);

        MaterializedResult materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 3L);

        materializedRows2 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_2 ORDER BY name, ds");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 13L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 12L);

        queryRunner.execute(getSession(), "INSERT INTO test_mqt_fact values (10, 100, '2015-02-01', 22), (11, 100, '2015-02-02', 22), (12, 200, '2015-02-02', 23)");

        // Refresh both materialized tables with predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table(" +
                "'raptor.tpch.test_refresh_mqt_join_1', " +
                "'{\"raptor.tpch.test_mqt_fact\":\"ds > ''2015-02-01''\"}', " +
                "'ds > ''2015-02-01''')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table(" +
                "'raptor.tpch.test_refresh_mqt_join_2', " +
                "'{\"raptor.tpch.test_mqt_fact\":\"ds > ''2015-02-01''\"}', " +
                "'ds > ''2015-02-01''')");

        materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 5L);

        materializedRows1 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_1 ORDER BY name, ds");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 35L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 12L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 23L);
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(0), "Customer3");
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(2), 14L);

        materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows2 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_2 ORDER BY name, ds");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 35L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 12L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 23L);

        queryRunner.execute(getSession(), "INSERT INTO test_mqt_fact values (20, 100, '2015-02-01', 100), (21, 100, '2015-02-02', 122), (22, 200, '2015-02-03', 123)");

        // Refresh both materialized tables with predicate
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table(" +
                "'raptor.tpch.test_refresh_mqt_join_1', " +
                "'{\"raptor.tpch.test_mqt_fact\":\"ds = ''2015-02-02'' OR ds = ''2015-02-03''\"}', " +
                "'ds > ''2015-02-01''')");
        queryRunner.execute(getSession(), "CALL system.runtime.refresh_materialized_query_table(" +
                "'raptor.tpch.test_refresh_mqt_join_2', " +
                "'{\"raptor.tpch.test_mqt_fact\":\"ds = ''2015-02-02''\", \"raptor.tpch.test_mqt_cust\":\"cust_id = 100\"}', " +
                "'ds = ''2015-02-02'' AND name = ''Customer1''')");

        materializedRows1 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 6L);

        materializedRows1 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_1 ORDER BY name, ds");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(1).getField(2), 157L);
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows1.getMaterializedRows().get(2).getField(2), 12L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 23L);
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(1), "2015-02-03");
        assertEquals(materializedRows1.getMaterializedRows().get(4).getField(2), 123L);
        assertEquals(materializedRows1.getMaterializedRows().get(5).getField(0), "Customer3");
        assertEquals(materializedRows1.getMaterializedRows().get(5).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(5).getField(2), 14L);

        materializedRows2 = computeActual("SELECT COUNT(1) FROM test_refresh_mqt_join_2");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), 4L);

        materializedRows2 = computeActual("SELECT name, ds, sum FROM test_refresh_mqt_join_2 ORDER BY name, ds");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(0).getField(2), 21L);
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(0), "Customer1");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(1), "2015-02-02");
        assertEquals(materializedRows2.getMaterializedRows().get(1).getField(2), 157L);
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(0), "Customer2");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(1), "2015-02-01");
        assertEquals(materializedRows2.getMaterializedRows().get(2).getField(2), 12L);
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(0), "Customer2");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(1), "2015-02-02");
        assertEquals(materializedRows1.getMaterializedRows().get(3).getField(2), 23L);
    }

    @Test
    public void testShardUuidHiddenColumn()
            throws Exception
    {
        assertUpdate("CREATE TABLE test_shard_uuid AS SELECT orderdate, orderkey FROM orders", "SELECT count(*) FROM orders");
        MaterializedResult actualResults = computeActual("SELECT *, \"$shard_uuid\" FROM test_shard_uuid");
        assertEquals(actualResults.getTypes(), ImmutableList.of(DATE, BIGINT, VARCHAR));
        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        String arbitraryUuid = null;
        for (MaterializedRow row : actualRows) {
            Object uuid = row.getField(2);
            assertInstanceOf(uuid, String.class);
            // check that the string can be parsed into a UUID
            UUID.fromString((String) uuid);
            arbitraryUuid = (String) uuid;
        }
        assertNotNull(arbitraryUuid);

        actualResults = computeActual(format("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = '%s'", arbitraryUuid));
        assertNotEquals(actualResults.getMaterializedRows().size(), 0);
        actualResults = computeActual("SELECT * FROM test_shard_uuid where \"$shard_uuid\" = 'foo'");
        assertEquals(actualResults.getMaterializedRows().size(), 0);
    }

    @Test
    public void testTableProperties()
            throws Exception
    {
        computeActual("CREATE TABLE test_table_properties_1 (foo BIGINT, bar BIGINT, ds DATE) WITH (ordering=array['foo','bar'], temporal_column='ds')");
        computeActual("CREATE TABLE test_table_properties_2 (foo BIGINT, bar BIGINT, ds DATE) WITH (ORDERING=array['foo','bar'], TEMPORAL_COLUMN='ds')");
    }

    @Test
    public void testShardsSystemTable()
            throws Exception
    {
        assertQuery("" +
                        "SELECT table_schema, table_name, sum(row_count)\n" +
                        "FROM system.shards\n" +
                        "WHERE table_schema = 'tpch'\n" +
                        "  AND table_name IN ('orders', 'lineitem')\n" +
                        "GROUP BY 1, 2",
                "" +
                        "SELECT 'tpch', 'orders', (SELECT count(*) FROM orders)\n" +
                        "UNION ALL\n" +
                        "SELECT 'tpch', 'lineitem', (SELECT count(*) FROM lineitem)");
    }

    @Test
    public void testCreateBucketedTable()
            throws Exception
    {
        assertUpdate("" +
                        "CREATE TABLE orders_bucketed " +
                        "WITH (bucket_count = 50, bucketed_on = ARRAY ['orderkey']) " +
                        "AS SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50");

        assertUpdate("INSERT INTO orders_bucketed SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM orders_bucketed", "SELECT * FROM orders UNION ALL SELECT * FROM orders");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) * 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");

        assertQuery("SELECT count(*) FROM orders_bucketed a JOIN orders_bucketed b USING (orderkey)", "SELECT count(*) * 4 FROM orders");

        assertUpdate("DELETE FROM orders_bucketed WHERE orderkey = 37", 2);
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT (count(*) * 2) - 2 FROM orders");
        assertQuery("SELECT count(DISTINCT \"$shard_uuid\") FROM orders_bucketed", "SELECT 50 * 2");

        assertUpdate("DROP TABLE orders_bucketed");
    }
}
