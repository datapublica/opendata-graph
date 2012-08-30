package com.datapublica.commoncrawl.aggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Same as NumericAggregationMapper except that it is dedicated for a specific input in which the line patterns are :
 * OPENDATA {site} {path} {count} and this mapper filters the keys by removing the {path} part
 */
public class NumericAggregationWithFilterMapper extends MapReduceBase implements
                Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line
        // Pattern : OPENDATA {site} {path} {count}
        String[] lineItems = line.split("\t");

        // Build the filtered output key
        String outputKey = lineItems[0];

        for (int i = 1; i < lineItems.length - 1; i++) {
            // skip the 3rd part
            if (i == 2) {
                continue;
            }
            outputKey = outputKey + "\t" + lineItems[i];
        }

        // Consider the last split as a long value (the one to be aggregated)
        long outputValue = Long.parseLong(lineItems[lineItems.length - 1]);

        // Output results
        output.collect(new Text(outputKey), new LongWritable(outputValue));

    }
}
