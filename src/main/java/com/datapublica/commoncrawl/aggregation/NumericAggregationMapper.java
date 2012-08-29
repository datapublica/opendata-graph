/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.aggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class NumericAggregationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line into two splits
        // Pattern : {domain} {pagesCount}
        String[] lineItems = line.split("\t");

        // Get the domain
        String outputKey = lineItems[0];

        for (int i = 1; i < lineItems.length - 1; i++) {
            outputKey = outputKey + "\t" + lineItems[i];
        }

        // Get the pages count
        long outputValue = Long.parseLong(lineItems[lineItems.length - 1]);

        // Output results
        output.collect(new Text(outputKey), new LongWritable(outputValue));

    }
}
