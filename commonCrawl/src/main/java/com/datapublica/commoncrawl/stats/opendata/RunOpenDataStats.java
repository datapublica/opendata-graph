package com.datapublica.commoncrawl.stats.opendata;

import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;

import com.datapublica.commoncrawl.aggregation.Aggregation;
import com.datapublica.commoncrawl.indexing.OpenDataIndexMapper;
import com.datapublica.commoncrawl.utils.JobHelper;
import com.datapublica.commoncrawl.utils.Loggers;

public class RunOpenDataStats {

    private final static Log LOG = LogFactory.getLog(RunOpenDataStats.class);

    public static final String OPENDATA_STATS_PATH_SUFFIX = "opendata/stats/";

    public static final String OPENDATA_INDEX_PATH_SUFFIX = "opendata/index/";

    public static final String OPENDATA_SITES_PATH_SUFFIX = "opendata/sites/";

    public static final String AGGR_OUTPUT_PATH_SUFFIX = "emr/french-index-aggredated/";

    public static void main(String[] args) throws IOException {

        Loggers.setup();

        // Set output paths
        String aggregationInput = JobHelper.DP_BUCKET_PREFIX + AGGR_OUTPUT_PATH_SUFFIX;
        String aggregationOutput = JobHelper.DP_BUCKET_PREFIX + OPENDATA_SITES_PATH_SUFFIX;
        String statsOutput = JobHelper.DP_BUCKET_PREFIX + OPENDATA_STATS_PATH_SUFFIX;
        String indexOutput = JobHelper.DP_BUCKET_PREFIX + OPENDATA_INDEX_PATH_SUFFIX;

        LOG.info("Extracting sites - pageCounts");
        Aggregation.RunNumericAggregationWithFilter(aggregationInput, aggregationOutput);

        JobConf finishStatsjob = new JobConf();
        JobHelper.ConfigureNumericOutputJob(finishStatsjob, aggregationOutput, statsOutput);
        finishStatsjob.setMapperClass(OpenDataStatsMapper.class);
        finishStatsjob.setCombinerClass(IdentityReducer.class);
        finishStatsjob.setReducerClass(SitesPageRatesReducer.class);
        finishStatsjob.setOutputValueClass(DoubleWritable.class);

        LOG.info("Extracting opendata sites and page counts/rates");
        JobClient.runJob(finishStatsjob);

        JobConf filterOpenDataIndexJob = new JobConf();
        filterOpenDataIndexJob.setMapperClass(OpenDataIndexMapper.class);
        JobHelper.ConfigureTextualOutputJob(filterOpenDataIndexJob, aggregationInput, indexOutput);

        LOG.info("Extracting open data paths");
        JobClient.runJob(filterOpenDataIndexJob);
    }
}
