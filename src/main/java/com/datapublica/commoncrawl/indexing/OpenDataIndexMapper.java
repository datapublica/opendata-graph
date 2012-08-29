/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.indexing;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class OpenDataIndexMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    private static final String OPENDATA = "OPENDATA";

    private static final String TEXT_PREFIX = "textData-";

    private static final String META_PREFIX = "metadata-";

    private static final String PATH_PREFIX = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/";

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
                    throws IOException {

        // Read the value as a line
        String line = value.toString();

        if (line.startsWith(OPENDATA)) {

            // Split the line into two splits
            String openDatasite = line.split("\t")[1];

            String textPath = line.split("\t")[2];

            // Clean prefix
            textPath = StringUtils.remove(textPath, PATH_PREFIX);

            String metaPath = StringUtils.replace(textPath, TEXT_PREFIX, META_PREFIX);

            // Output results
            output.collect(new Text(openDatasite), new Text(metaPath));
        }
    }
}
