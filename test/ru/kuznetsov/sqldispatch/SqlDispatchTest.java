package ru.kuznetsov.sqldispatch;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import ru.kuznetsov.sqldispatch.SqlFilter.FilterType;
import ru.kuznetsov.sqldispatch.SqlFilterGroup.FilterGroupType;

public class SqlDispatchTest {
    SqlDispatch sqlDispatch;

    @Before
    public void setup() {
        List<String> metrics = new ArrayList<>();
        List<String> dimensions = new ArrayList<>();
        dimensions.add("dimension");
        dimensions.add("dimension1");

        metrics.add("metric");
        metrics.add("metric1");
        metrics.add("metric2");

        DruidDatasource defaultDatasource = new DruidDatasource("table1", dimensions, metrics);
        sqlDispatch = new SqlDispatch();
        sqlDispatch.getDatasources().add(defaultDatasource);
    }
	
    @Test
    public void simpleSplitWordsTest() throws SQLException {
        String query = "SELECT dimension, metric FROM table1";
        sqlDispatch.dispatch(query);
        assertArrayEquals(new String[]{"SELECT","dimension", ",", "metric", "FROM", "table1"}, sqlDispatch.getWords());
    }

    @Test
    public void simpleSplitWordsTest1() throws SQLException {
        String query = "SELECT dimension , metric FROM table1";
        sqlDispatch.dispatch(query);
        assertArrayEquals(new String[]{"SELECT","dimension", ",", "metric", "FROM", "table1"}, sqlDispatch.getWords());
    }

    @Test
    public void simpleSplitWordsTest3() throws SQLException {
        String query = "SELECT dimension ,metric\n FROM table1";
        sqlDispatch.dispatch(query);
        assertArrayEquals(new String[]{"SELECT","dimension", ",", "metric", "FROM", "table1"}, sqlDispatch.getWords());
    }
	
    @Test
    public void simpleSplitWordsTestBrackets() throws SQLException {
        String query = "SELECT dimension ,metric FROM table1 WHERE metric IN (test, value)";
        sqlDispatch.dispatch(query);

        for (String str : sqlDispatch.getWords()) {
            System.out.print(str + " ");
        }
        System.out.println("");
        assertArrayEquals(new String[]{"SELECT","dimension", ",", "metric", "FROM", "table1",
                "WHERE", "metric", "IN", "(", "test", ",", "value", ")"}, sqlDispatch.getWords());
    }

    @Test
    public void simpleSplitWordsTest2() throws SQLException {
        String query = "SELECT dimension ,metric FROM table1";
        sqlDispatch.dispatch(query);
        assertArrayEquals(new String[]{"SELECT","dimension", ",", "metric", "FROM", "table1"}, sqlDispatch.getWords());
    }

    @Test
    public void testQueryDispatch() throws SQLException {
        String query = "SELECT dimension, metric FROM table1";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());
    }
	
    @Test
    public void testWhereQueryDispatch() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension = 1";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.Equals, "1");
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereQueryDispatch1() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension < 1";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.Less, "1");
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereQueryDispatch1a() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension > 1";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.Greater, "1");
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereQueryDispatch2() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension <> 1";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.NotEquals, "1");
        SqlFilterGroup filterGroup = new SqlFilterGroup(filter);
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereQueryDispatch3() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension LIKE text";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.Like, "text");
        SqlFilterGroup filterGroup = new SqlFilterGroup(filter);
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
    }
	
    @Test
    public void testWhereQueryDispatch4() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension in (text)";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter = new SqlFilter("dimension", FilterType.in, new String[]{"text"});
        SqlFilterGroup filterGroup = new SqlFilterGroup(filter);
        assertTrue(filter.equals(sqlDispatch.getSqlFilter()));
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
    }
	
    @Test
    public void testWhereSimpleAndQueryDispatch() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension <> 1 AND dimension > 3";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter1 = new SqlFilter("dimension", FilterType.NotEquals, "1");
        SqlFilterGroup filterGroup1 = new SqlFilterGroup(filter1);
        SqlFilter filter2 = new SqlFilter("dimension", FilterType.Greater, "3");
        SqlFilterGroup filterGroup2 = new SqlFilterGroup(filter2);
        SqlFilterGroup filterGroup = new SqlFilterGroup(FilterGroupType.AND, filterGroup1, filterGroup2);
        //assertTrue(filter1.equals(sqlDispatch.getSqlFilter()));
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereTwiceAndQueryDispatch() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension <> 1 AND dimension > 3 AND  dimension < 6";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter1 = new SqlFilter("dimension", FilterType.NotEquals, "1");
        SqlFilterGroup filterGroup1 = new SqlFilterGroup(filter1);
        SqlFilter filter2 = new SqlFilter("dimension", FilterType.Greater, "3");
        SqlFilterGroup filterGroup2 = new SqlFilterGroup(filter2);
        SqlFilter filter3 = new SqlFilter("dimension", FilterType.Less, "6");
        SqlFilterGroup filterGroup3 = new SqlFilterGroup(filter3);
        SqlFilterGroup filterGroup = new SqlFilterGroup(FilterGroupType.AND, new SqlFilterGroup(FilterGroupType.AND, filterGroup1, filterGroup2), filterGroup3);
        //assertTrue(filter1.equals(sqlDispatch.getSqlFilter()));
        System.out.println("Actual: " + sqlDispatch.getRootSqlFilterGroup().print());
        System.out.println("Expected: " + filterGroup.print());
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
        //assertEquals("table1", sqlDispatch.);
    }
	
    @Test
    public void testWhereSimpleOrQueryDispatch() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension <> 1 OR dimension > 3";

        sqlDispatch.dispatch(query);

        assertEquals("table1", sqlDispatch.getDataSource());
        assertEquals(1, sqlDispatch.getQueryDimensions().size());
        assertEquals("dimension", sqlDispatch.getQueryDimensions().get(0));
        assertEquals(1, sqlDispatch.getQueryMetrics().size());
        assertEquals("metric", sqlDispatch.getQueryMetrics().get(0));
        assertEquals("table1", sqlDispatch.getDataSource());

        SqlFilter filter1 = new SqlFilter("dimension", FilterType.NotEquals, "1");
        SqlFilterGroup filterGroup1 = new SqlFilterGroup(filter1);
        SqlFilter filter2 = new SqlFilter("dimension", FilterType.Greater, "3");
        SqlFilterGroup filterGroup2 = new SqlFilterGroup(filter2);
        SqlFilterGroup filterGroup = new SqlFilterGroup(FilterGroupType.OR, filterGroup1, filterGroup2);
        //assertTrue(filter1.equals(sqlDispatch.getSqlFilter()));
        assertTrue(filterGroup.equals(sqlDispatch.getRootSqlFilterGroup()));
        //assertEquals("table1", sqlDispatch.);
    }

}
