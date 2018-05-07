package ru.kuznetsov.sqldispatch;

import java.util.List;

/**
 *
 * @author Kuznetsov Igor
 */
public class DruidDatasource {
    private String datasouceName;
    private List<String> dimensions;
    private List<String> metrics;

    public DruidDatasource(String datasouceName, List<String> dimensions, List<String> metrics) {
        this.datasouceName = datasouceName;
        this.dimensions = dimensions;
        this.metrics = metrics;
    }

    public String getDatasouceName() {
        return datasouceName;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public List<String> getMetrics() {
        return metrics;
    }
}
