/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.stats.opendata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapping class that produces the normalized domain name and a count of '1' for every successfully retrieved URL in the
 * Common Crawl corpus. Actually we consider only the url passed as a Key for the map method. Further we might process
 * the json metadata contained in Value
 */
public class OpenDataStatsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line into two splits
        // Pattern : {domain} {pagesCount}
        String[] lineItems = line.split("\t");

        // Get the domain
        String openDataSite = lineItems[1];

        // Get the pages count
        long outputValue = Long.parseLong(lineItems[lineItems.length - 1]);

        // Output results
        output.collect(new Text(openDataSite), new LongWritable(outputValue));

    }
}
