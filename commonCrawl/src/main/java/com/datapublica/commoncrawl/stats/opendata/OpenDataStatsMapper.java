package com.datapublica.commoncrawl.stats.opendata;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper that outputs a website name and its pagecount
 */
public class OpenDataStatsMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line
        // Pattern : OPENDATA {domain} {pagesCount}
        String[] lineItems = line.split("\t");

        // Get the domain
        String openDataSite = lineItems[1];

        // Get the pages count
        long outputValue = Long.parseLong(lineItems[2]);

        // Output results
        output.collect(new Text(openDataSite), new LongWritable(outputValue));

    }
}
