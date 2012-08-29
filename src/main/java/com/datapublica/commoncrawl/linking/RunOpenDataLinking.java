/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.linking;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import com.datapublica.commoncrawl.aggregation.Aggregation;
import com.datapublica.commoncrawl.utils.Loggers;
import com.datapublica.commoncrawl.utils.JobHelper;

public class RunOpenDataLinking {

    private final static Log LOG = LogFactory.getLog(RunOpenDataLinking.class);

    public static final String INTERMEDIATE_OUTPUT_PATH = "opendata/links";

    public static final String FINAL_OUTPUT_PATH = "opendata/links";

    public static final Set<String> openDataSites = new HashSet<String>(
                    JobHelper.getListFromResource("opendata-sites.txt"));

    public static void main(String[] args) throws IOException {

        // Initialize logger configuration
        Loggers.setup();

        // Run job
        RunOpenDataLinking frenchWebIndexingJob = new RunOpenDataLinking();
        frenchWebIndexingJob.run();

    }

    /**
     * Runs the OpenData linking job depending on a subset or on the full corpus depending on the args. If there are no
     * args passed, run across the whole corpus
     * 
     * @param args Usage : inputPath outputPath
     */
    private void run() {

        // Run the job over the whole data set
        runOnFullCorpus();

        // Once finished aggregate the results by calling an aggregation job
        // The aim of this aggregation is to gather all the results in one folder
        String aggregationInput = JobHelper.DP_BUCKET_PREFIX + INTERMEDIATE_OUTPUT_PATH + "*/*";
        String aggregationOutput = JobHelper.DP_BUCKET_PREFIX + FINAL_OUTPUT_PATH;
        Aggregation.RunTextualAggregation(aggregationInput, aggregationOutput);

    }

    public static void runOnFullCorpus() {

        Loggers.setup();

        // Input and output paths, mandatory to create jobs
        String inputPath = null;
        String outputPath;

        // Generating an individual output folder for each segment
        outputPath = JobHelper.DP_BUCKET_PREFIX + INTERMEDIATE_OUTPUT_PATH;

        try {
            // Creates a new job configuration for this HADOOP job.
            JobConf jobConf = new JobConf();
            JobHelper.setOpenDataInputPaths(jobConf);
            JobHelper.ConfigureTextualOutputJob(jobConf, inputPath, outputPath);
            jobConf.setInputFormat(SequenceFileInputFormat.class);
            jobConf.setMapperClass(OpenDataLinkingMapper.class);

            // Run the job
            LOG.info("Finished Initialization");
            JobClient.runJob(jobConf);

            // Announce finishing
            LOG.info("Finished Job");
        } catch (IOException e) {
            LOG.error("Failed to run the job " + e);
        }

    }
}
