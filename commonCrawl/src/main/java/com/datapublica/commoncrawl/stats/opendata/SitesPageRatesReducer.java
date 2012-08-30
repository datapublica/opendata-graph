package com.datapublica.commoncrawl.stats.opendata;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer that gets a website name as a key and 2 pagecounts, supposing that one of them is the total page count of
 * this site, and the second is the count of pages that talk about open data. The aim of this reducer is to filter the
 * raw open data sites and output only the sites that have a total pagecount and openCount/total count beyond some
 * thresholds. These thresholds were defined after a study on many sites.
 */
public class SitesPageRatesReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, DoubleWritable> {

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

            String openDataSite = key.toString();

            // find the maximum
            double max = pageCount[0];
            double min = pageCount[1];
            if (pageCount[1] > max) {
                max = pageCount[1];
                min = pageCount[0];
            }

            // double outValue = min / max;
            double outValue = Double.valueOf(new DecimalFormat("#.##").format(min / max).replace(',', '.'));

            // After analyzing many results previously, we considered this threshold
            if (max > 5) {
                if (outValue > 0.5) {
                    DoubleWritable result = new DoubleWritable(outValue);
                    output.collect(new Text(openDataSite + "\t" + min + "\t" + max), result);
                }
            }
        }

    }
}
