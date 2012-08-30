package com.datapublica.commoncrawl.aggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * A mapper for generic usage to aggregate similar input from several locations into one single location. The supported
 * input is basically <Text,LongWritable> pairs written in text files. the output is meant to aggregate their values and
 * put them in one location.
 */
public class NumericAggregationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line
        // Pattern : {keyPart1}\t[{keyPart2}\t...] {pagesCount}
        String[] lineItems = line.split("\t");

        // Rebuild the key
        String outputKey = lineItems[0];

        for (int i = 1; i < lineItems.length - 1; i++) {
            outputKey = outputKey + "\t" + lineItems[i];
        }

        // Consider the last split as a long value (one to be aggregated)
        long outputValue = Long.parseLong(lineItems[lineItems.length - 1]);

        // Output results
        output.collect(new Text(outputKey), new LongWritable(outputValue));

    }
}
