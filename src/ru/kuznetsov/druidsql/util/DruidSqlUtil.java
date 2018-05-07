package ru.kuznetsov.druidsql.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import ru.kuznetsov.sqldispatch.DruidDatasource;

/**
 *
 * @author Kuznetsov Igor
 */
public class DruidSqlUtil {
    private final static String BROKER_PATH = "/druid/v2/datasources/";
    private final static String QUERY_PATH = "/druid/v2";
    
    public static List<String> getDatasources (String endpoint) throws Throwable {
        
        String resp = get(endpoint + BROKER_PATH);
        //System.out.println(resp);
        
        Gson gson = new GsonBuilder().create();
        Type type = new TypeToken<List<String>>(){}.getType();
        
        List<String> list = gson.fromJson(resp, type);
        //System.out.println(list.toString());
        return list;
    }
    
    public static List<String> getMetrics (String endpoint, String datasource) throws Throwable {
        String resp = get(endpoint + BROKER_PATH + datasource + "/metrics");
        Gson gson = new GsonBuilder().create();
        Type type = new TypeToken<List<String>>(){}.getType();
        
        List<String> list = gson.fromJson(resp, type);
        return list;
    }
    
    public static List<String> getDimensions (String endpoint, String datasource) throws Throwable {
        String resp = get(endpoint + BROKER_PATH + datasource + "/dimensions");
        Gson gson = new GsonBuilder().create();
        Type type = new TypeToken<List<String>>(){}.getType();
        
        List<String> list = gson.fromJson(resp, type);
        return list;
    }
    
    private static String get(String urlString) throws Throwable {
        URL url = new URL(urlString);
        HttpURLConnection conn = (HttpURLConnection)(url).openConnection();
        conn.setRequestProperty("Content-Type", "text/json; charset=UTF-8");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setRequestMethod("GET");
        
        return readResponse(conn);
    }
    
    public static String post(String urlString, String content, String contentType, String charset) throws Throwable
    {
        byte[] requestData = content.getBytes(charset);
        
        URL url = new URL(urlString);
        HttpURLConnection connection = (HttpURLConnection)url.openConnection();
        
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", contentType);
        connection.setRequestProperty("Content-Length", "" + requestData.length);
        connection.setDoInput(true);
        connection.setDoOutput(true);
        
        try (OutputStream os = connection.getOutputStream())
        {
            os.write(requestData);
            os.flush();
        }        
                        
        return readResponse(connection);
    }
    
    private static String readResponse(HttpURLConnection connection) throws Throwable {
        try (InputStream is = connection.getInputStream())
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();                
            byte[] buf = new byte[8192];
            int len = is.read(buf, 0, buf.length);
            while (len > 0)
            {
                baos.write(buf, 0, len);
                len = is.read(buf, 0, buf.length);
            }
            baos.flush();                
            String res = new String(baos.toByteArray(), "UTF-8");
            baos.close();
            return res;
        }
        catch (Throwable t)
        {
            if (connection.getErrorStream() != null)
            {
                InputStream errorStream = connection.getErrorStream();
                byte[] errorData = new byte[
                    connection.getContentLength() > 0 ? connection.getContentLength() : errorStream.available()
                ];  
                errorStream.read(errorData, 0, errorData.length);
                System.out.println("WWWW " + new String(errorData, "UTF-8"));
                throw new Exception(new String(errorData, "UTF-8"));
            }
            else
            {
                throw t;
            }
        }
    }
    
    public static List<DruidDatasource> prepareDatasourcesList(String endpoint) throws Throwable {
        List<String> datasourceNames = getDatasources(endpoint);
        List<DruidDatasource> datasourcesList = new ArrayList<>();
        
        for(String datasource : datasourceNames) {
            List<String> dimensions = getDimensions(endpoint, datasource);
            List<String> metrics = getMetrics(endpoint, datasource);
            datasourcesList.add(new DruidDatasource(datasource, dimensions, metrics));
        }
        return datasourcesList;
    }
    
    public static String executeQuery(String endpoint, String query) throws Throwable {
        String res;
        res = post(endpoint + QUERY_PATH, query, "application/json; charset=utf-8", "UTF-8");
        return res;
    }
}
