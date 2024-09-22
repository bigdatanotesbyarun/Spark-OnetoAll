package com.spark3.x;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SalesData {
    private String date;
    private String region;
    private int sales;

    public SalesData(String date, String region, int sales) {
        this.date = date;
        this.region = region;
        this.sales = sales;
    }

    public String getDate() {
        return date;
    }

    public String getRegion() {
        return region;
    }

    public int getSales() {
        return sales;
    }
}
