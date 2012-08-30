package com.datapublica.commoncrawl.aggregation;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * a mapper for generic usage to merge and aggregate similar input from different locations into one single location
 */
public class TextualAggregationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        // Split the line
        // Pattern : {keyPart1}\t[{keyPart2}\t...] {textValue}
        String[] lineItems = line.split("\t");

        // Rebuild the output key
        String outputKey = lineItems[0];

        for (int i = 1; i < lineItems.length - 1; i++) {
            outputKey = outputKey + "\t" + lineItems[i];
        }

        // Get the textual output value
        String outputValue = lineItems[lineItems.length - 1];

        // Output results
        output.collect(new Text(outputKey), new Text(outputValue));

    }
}
