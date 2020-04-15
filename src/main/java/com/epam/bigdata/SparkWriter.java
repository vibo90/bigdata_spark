package com.epam.bigdata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;

public class SparkWriter {
    public static void write(Dataset<Row> rowDataset, Format format, String filePath, String partition){
        switch (format){
            case Avro:
                if (partition == null || partition.isEmpty()){
                    rowDataset.write().format("com.databricks.spark.avro").mode(SaveMode.Overwrite).save(filePath);
                }
                break;
            case Orc:
                if (partition == null || partition.isEmpty()){
                    rowDataset.write().mode(SaveMode.Overwrite).orc(filePath);
                }
                else{
                    rowDataset.write().mode(SaveMode.Overwrite).partitionBy(partition).orc(filePath);
                }

                break;
            case Parquet:
                if (partition == null || partition.isEmpty()){
                    rowDataset.write().mode(SaveMode.Overwrite).parquet(filePath);
                }
                else{
                    rowDataset.write().mode(SaveMode.Overwrite).partitionBy(partition).parquet(filePath);
                }
                break;
            default:
                throw new IllegalArgumentException("File format not defined");
        }
    }
}
