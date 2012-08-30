package com.datapublica.commoncrawl.indexing;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * A Mapper used to filter out the OpenData sites paths from the raw french indexing output and generate their relative
 * paths to metadata files. Important : this approach is based on the fact that all the urls in a given textData-{ID}
 * are the same in a metadata-{ID}. See the discussion group :
 * https://groups.google.com/forum/?fromgroups=#!topic/common-crawl/3zllUZ1ZSrI
 */
public class OpenDataIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final String OPENDATA = "OPENDATA";

    private static final String TEXT_PREFIX = "textData-";

    private static final String META_PREFIX = "metadata-";

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        if (line.startsWith(OPENDATA)) {

            // Split the line and get the sites and their paths
            String openDatasite = line.split("\t")[1];
            String textPath = line.split("\t")[2];

            // Generate the relative path to the metadata files
            String metaPath = StringUtils.replace(textPath, TEXT_PREFIX, META_PREFIX);

            // Output results
            output.collect(new Text(openDatasite), new Text(metaPath));
        }
    }
}
