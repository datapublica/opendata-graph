/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.indexing;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;

import com.datapublica.commoncrawl.aggregation.Aggregation;
import com.datapublica.commoncrawl.utils.Loggers;
import com.datapublica.commoncrawl.utils.JobHelper;

public class RunFrenchWebIndexing {

    private static final Log LOG = LogFactory.getLog(RunFrenchWebIndexing.class);

    private static final String INTERMEDIATE_OUTPUT_PATH = "emr/index/raw/";

    private static final String FINAL_OUTPUT_PATH = "index/raw/";

    private static final String OPENDATA_INDEX_PATH = "opendata/index/";

    public static void main(String[] args) throws IOException {

        // Initialize logger configuration
        Loggers.setup();

        // Run job
        RunFrenchWebIndexing frenchWebIndexingJob = new RunFrenchWebIndexing();
        frenchWebIndexingJob.run(args);

    }

    /**
     * Runs the French web indexing job depending on a subset or on the full corpus depending on the args. If there are
     * no args passed, run across the whole corpus
     * 
     * @param args Usage : inputPath outputPath
     */
    private void run(String[] args) {

        // Check if arguments are used to specify input and output
        if (args.length == 2) {
            // Run job on [specific] subset with a specified output path
            runOnSpecificInput(args);

        } else {
            // Run the job over the whole data set
            runOnFullCorpus();

            // Once finished aggregate the results by calling an aggregation job
            // The aim of this aggregation is to gather all the results in one folder
            String aggregationInput = JobHelper.DP_BUCKET_PREFIX + INTERMEDIATE_OUTPUT_PATH + "*/*";
            String aggregationOutput = JobHelper.DP_BUCKET_PREFIX + FINAL_OUTPUT_PATH;
            Aggregation.RunNumericAggregation(aggregationInput, aggregationOutput);

            // Filter out the open data sites index
            runOpenDataIndexFiltering();
        }
    }

    /**
     * Run the language statistics on a specific subset or common crawl and output the results to a specified output
     * path.
     * 
     * @param args Usage : {segmentID}/[textData-{fileID}] outputPath
     */
    private void runOnSpecificInput(String[] args) {
        // Append the input path from the first argument
        String inputPath = JobHelper.COMMONCRAWL_DATASET_PREFIX + args[0];

        // Append the output path from the second argument
        String outputPath = JobHelper.DP_BUCKET_PREFIX + args[1];

        // Create a generic statistical job configuration for this Hadoop job
        JobConf jobConf = new JobConf();
        JobHelper.ConfigureNumericOutputJob(jobConf, inputPath, outputPath);

        // Set custom configuration
        jobConf.setMapperClass(FrenchWebIndexingMapper.class);
        jobConf.setInputFormat(SequenceFileInputFormat.class);

        try {
            LOG.info("Running job" + jobConf.getJobName());
            LOG.info("Input path" + inputPath);
            LOG.info("Output path" + outputPath);
            JobClient.runJob(jobConf);
            LOG.info("Finished successfully " + jobConf.getJobName());
        } catch (IOException e) {
            LOG.error("Failed to run the job, caught error : " + e);
        }
    }

    private void runOnFullCorpus() {
        List<String> validSegments = JobHelper.getValidSegments();

        // Create and run the jobs sequentially one after another
        int i = 0;
        long startTime, finishTime;
        for (String segment : validSegments) {

            // Set the whole segment as an input path and take textData files only
            String inputPath = JobHelper.COMMONCRAWL_DATASET_PREFIX + segment + "/textData-*";

            // Generate an individual output folder for each segment
            String outputPath = JobHelper.DP_BUCKET_PREFIX + INTERMEDIATE_OUTPUT_PATH + segment + "/";

            // Create a generic statistical job configuration for this Hadoop job
            JobConf jobConf = new JobConf();
            JobHelper.ConfigureNumericOutputJob(jobConf, inputPath, outputPath);

            // Set custom configuration
            jobConf.setInputFormat(SequenceFileInputFormat.class);
            jobConf.setMapperClass(FrenchWebIndexingMapper.class);

            // Run the job
            LOG.info("=================================");
            LOG.info("Running French indexing job : " + ++i + "/" + (validSegments.size()));
            LOG.info("Segment : " + segment);
            startTime = System.currentTimeMillis();
            LOG.info("Start time : " + startTime);
            try {
                JobClient.runJob(jobConf);
                finishTime = System.currentTimeMillis();
                LOG.info("Finish time : " + System.currentTimeMillis());
                LOG.info("Duration " + (finishTime - startTime));
                LOG.info("Finished Job");
                LOG.info("=================================");
            } catch (IOException e) {
                LOG.error("Failed to run the job, caught error : " + e);
                return;
            }
        }
    }

    private void runOpenDataIndexFiltering() {
        // Append the input path from the first argument
        String inputPath = JobHelper.DP_BUCKET_PREFIX + FINAL_OUTPUT_PATH;

        // Append the output path from the second argument
        String outputPath = JobHelper.DP_BUCKET_PREFIX + OPENDATA_INDEX_PATH;

        // Create a generic statistical job configuration for this Hadoop job
        JobConf jobConf = new JobConf();
        JobHelper.ConfigureTextualOutputJob(jobConf, inputPath, outputPath);

        // Set custom configuration
        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setMapperClass(OpenDataIndexMapper.class);

        try {
            LOG.info("Running Open Data Index Filtering job");
            LOG.info("Input path" + inputPath);
            LOG.info("Output path" + outputPath);
            JobClient.runJob(jobConf);
            LOG.info("Finished successfully " + jobConf.getJobName());
        } catch (IOException e) {
            LOG.error("Failed to run the job, caught error : " + e);
        }
    }
}
