package com.epam.bigdata;

import org.apache.spark.sql.SparkSession;

public class SparkSessinFactory {
  public static SparkSession create()
  {
     return SparkSession
              .builder()
              .master("local[*]")
              .appName("Expedia hotels")
              .config("spark.some.config.option", "some-value")
              // .config("spark.driver.bindAddress", "127.0.0.1")
              .getOrCreate();
  }
}
