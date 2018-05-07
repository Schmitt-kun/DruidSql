package ru.kuznetsov.druidsql.util;

import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Kuznetsov Igor
 */
public class DruidConnectTest {
    
    public DruidConnectTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    @Test
    public void druidConnectTest() throws Throwable {
        String endpoint = "http://lager.loyalgroup.pro:8082";
        
        List<String> res = DruidSqlUtil.getDatasources(endpoint);
        System.out.println(res.toString());
        
        for (String datasource : res) {
            List<String> metrics = DruidSqlUtil.getMetrics(endpoint, datasource);
            List<String> dimensions = DruidSqlUtil.getDimensions(endpoint, datasource);
            System.out.println(datasource + " { dimensions: " + dimensions.toString() + ", metrics: " + metrics.toString() + " }");
        }
    }
}
