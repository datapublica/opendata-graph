package com.datapublica.commoncrawl.stats.opendata;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class SitesPageRatesReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, DoubleWritable> {

    private MultipleOutputs mos;

    public void configure(JobConf conf) {

        mos = new MultipleOutputs(conf);
    }

    public void close() throws IOException {
        mos.close();
    }

    public void reduce(Text key, Iterator<LongWritable> values, OutputCollector<Text, DoubleWritable> output,
                    Reporter reporter) throws IOException {

        long[] pageCount = new long[2];
        int i = 0;

        while (values.hasNext()) {
            LongWritable val = values.next();
            pageCount[i] = val.get();
            i++;
        }

        if (pageCount[0] > 0 && pageCount[1] > 0) {

            String outKey = key.toString();
            // find the maximum
            double max = pageCount[0];
            double min = pageCount[1];
            if (pageCount[1] > max) {
                max = pageCount[1];
                min = pageCount[0];
            }

            // double outValue = min / max;
            double outValue = Double.valueOf(new DecimalFormat("#.##").format(min / max).replace(',', '.'));

            // After analyzing results previously, we considered this threshold
            if (max > 5) {
                if (outValue > 0.5) {
                    DoubleWritable result = new DoubleWritable(outValue);
                    output.collect(new Text(outKey + "\t" + min + "\t" + max), result);
                }
            }
        }

    }
}
