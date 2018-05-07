package ru.kuznetsov.druidsql;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import ru.kuznetsov.druidsql.util.DruidSqlUtil;
import ru.kuznetsov.sqldispatch.DruidDatasource;
import ru.kuznetsov.sqldispatch.SqlDispatch;

public class Main {

    public static void main(String[] args) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));

            System.out.print("Druid query endpoint: ");
            String endpoint = reader.readLine();

            if ("".equals(endpoint)) return;
            List<DruidDatasource> datasources = DruidSqlUtil.prepareDatasourcesList(endpoint);
            for (DruidDatasource ds : datasources) {
                System.out.println(ds.getDatasouceName() + " { dimensions: " + ds.getDimensions().toString()
                        + ", metrics: " + ds.getMetrics().toString() + " }");
            }

            System.out.print("Type query: ");
            String query = reader.readLine();

            if ("".equals(query)) return;


            //System.out.println("\"" + args[0] + "\"");
            List<String> metrics = new ArrayList<>();
            metrics.add("metric");
            metrics.add("metric1");
            List<String> dimensions = new ArrayList<>();
            dimensions.add("dimension");
            dimensions.add("dimension1");
            SqlDispatch sqlDispatch = new SqlDispatch();
            datasources.stream().forEach((ds) -> sqlDispatch.getDatasources().add(ds));
        
            sqlDispatch.dispatch(query);
            String json = sqlDispatch.toGson();
            System.out.println(json);
            String result = DruidSqlUtil.executeQuery(endpoint, json);
            System.out.println("Result:");
            System.out.println(result);
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }
}
