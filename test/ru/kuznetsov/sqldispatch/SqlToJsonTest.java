package ru.kuznetsov.sqldispatch;

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import ru.kuznetsov.sqldispatch.SqlFilter.FilterType;
import ru.kuznetsov.sqldispatch.SqlFilterGroup.FilterGroupType;


public class SqlToJsonTest {
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
    public void test() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension = 1";
        sqlDispatch.dispatch(query);

        System.out.println(sqlDispatch.toGson());
    }

    @Test
    public void testIn() throws SQLException {
        String query = "SELECT dimension, metric FROM table1 WHERE dimension in (1,2,3)";
        sqlDispatch.dispatch(query);

        System.out.println(sqlDispatch.toGson());
    }

    @Test
    public void convertGroupToString() throws SQLException {
        SqlFilter filter1 = new SqlFilter("dimension", FilterType.Equals, "1");
        SqlFilterGroup filterGroup1 = new SqlFilterGroup(filter1);
        SqlFilter filter2 = new SqlFilter("dimension", FilterType.Equals, "3");
        SqlFilterGroup filterGroup2 = new SqlFilterGroup(filter2);
        SqlFilterGroup rootGroup = new SqlFilterGroup(FilterGroupType.AND, filterGroup1, filterGroup2);

        System.out.println(sqlDispatch.filterGroupsToJson(rootGroup));
    }

}
