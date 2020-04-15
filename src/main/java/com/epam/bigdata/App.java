package com.epam.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Console;

public class App {
    public static void main(String[] args) {

        transformFormat(args[0], args[1]);

    }

    static void transformFormat(String source, String destination) {
        SparkSession session = SparkSessinFactory.create();
        Dataset<Row> ds = SparkReader.readFile(session, Format.csv, source);
        ds.printSchema();
        SparkWriter.write(ds, Format.Avro, destination, null);
        Console.print("Done");

    }

}
