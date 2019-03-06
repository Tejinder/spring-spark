package com.tejinder.service;


import static org.apache.spark.sql.functions.col;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.tejinder.bean.SparkCount;
import com.tejinder.bean.SparkWord;

@Component
public class SparkWordCount {
    @Autowired
    private SparkSession sparkSession;

    public List<SparkCount> count() {
        String input = "hello world hello hello hello this hello hello";
        String[] _words = input.split(" ");
        List<SparkWord> words = Arrays.stream(_words).map(SparkWord::new).collect(Collectors.toList());
        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, SparkWord.class);
        dataFrame.show();
        //StructType structType = dataFrame.schema();

        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
        groupedDataset.count().show();
        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
        return rows.stream().map(new Function<Row, SparkCount>() {
            @Override
            public SparkCount apply(Row row) {
                return new SparkCount(row.getString(0), row.getLong(1));
            }
        }).collect(Collectors.toList());
    }
}