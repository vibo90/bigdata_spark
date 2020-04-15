package com.epam.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkReader {
    public static Dataset<Row> readFile(SparkSession session, Format format, String filePath){
        switch (format){
            case Avro:
                return session.read().format("com.databricks.spark.avro").load(filePath).cache();
            case Orc:
                return session.read().orc(filePath).cache();
            case Parquet:
                return session.read().parquet(filePath).cache();
            case csv:
            return session.read().format("csv")
            .option("sep", ";")
            .option("inferSchema", "true")
            .option("header", "true")
            .load(filePath);
//            case Kafka:
//                return session.read().format("kafka").load();
            default:
                throw new IllegalArgumentException("File format not defined");
        }
    }
}
