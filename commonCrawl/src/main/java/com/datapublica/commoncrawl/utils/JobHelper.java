package com.datapublica.commoncrawl.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.tika.io.IOUtils;

import com.datapublica.commoncrawl.indexing.RunFrenchWebIndexing;

public class JobHelper {

    private final static Log LOG = LogFactory.getLog(JobHelper.class);

    public static final String DP_BUCKET_PREFIX = "s3n://" + JobHelper.getBucketName() + "";

    public static final String COMMONCRAWL_DATASET_PREFIX = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/";

    private static final String VALID_SEGMENTS_MANIF = "s3n://aws-publicdatasets/common-crawl/parse-output/valid_segments.txt";

    /**
     * Gets the list of the valid segments in common crawl 2012 corpus by loading it from its location on S3. See :
     * https://groups.google.com/forum/?fromgroups=#!topic/common-crawl/QYTmnttZZyo
     * 
     * @return Valid segment IDs list
     */
    public static List<String> getValidSegments() {

        List<String> validSegments = null;

        FileSystem fs;
        BufferedReader reader = null;
        try {
            validSegments = new ArrayList<String>();
            fs = FileSystem.get(new URI(VALID_SEGMENTS_MANIF), new Configuration());
            reader = new BufferedReader(new InputStreamReader(fs.open(new Path(VALID_SEGMENTS_MANIF))));
            String segmentId;
            while ((segmentId = reader.readLine()) != null) {
                validSegments.add(segmentId);
            }

        } catch (URISyntaxException e) {
            LOG.warn("Invalid path to valid segments list", e);
        } catch (IOException e) {
            LOG.error("Cannot access to valid segments manifest file", e);
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return validSegments;
    }

    /**
     * Sets all the OpenData paths which are in the "opendata-paths.txt" as Input for a {@link JobConf} This list is
     * gotten after executing the {@link RunFrenchWebIndexing} job. It was bundled with the resources to make any
     * further use independent.
     * 
     * @param jobConf
     * @throws IOException
     */
    public static void setOpenDataInputPaths(JobConf jobConf) throws IOException {

        List<String> openDataPaths = getListFromResource("opendata-paths.txt");
        for (String path : openDataPaths) {
            FileInputFormat.addInputPath(jobConf, new Path(COMMONCRAWL_DATASET_PREFIX + path));
        }

    }

    /**
     * Reads a resource file and returns its lines as a List<String>
     * 
     * @param resource resource name
     * @return resource file lines as a list
     */
    public static List<String> getListFromResource(String resource) {

        List<String> list = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(JobHelper.class.getResourceAsStream(resource)));
            String str;
            while ((str = reader.readLine()) != null) {
                list.add(str);
            }

        } catch (IOException e) {
            return null;
        } finally {
            IOUtils.closeQuietly(reader);
        }
        return list;

    }

    /**
     * Cleans an output path for a MapReduce job by deleting it if it exists
     * 
     * @param outputPath
     */
    public static void cleanOutputPath(String outputPath) {
        // Delete the output path directory if it already exists.
        // Hadoop refuses to write the output in an existing folder even if it is empty, or throws an exception
        FileSystem fs;
        try {
            fs = FileSystem.get(new URI(outputPath), new Configuration());
            if (fs.exists(new Path(outputPath)))
                fs.delete(new Path(outputPath), true);
        } catch (IOException e) {
            LOG.error("Couldn't output path" + outputPath);
            LOG.error("Got error message : " + e);
        } catch (URISyntaxException e) {
            LOG.error("Invalid output path" + outputPath);
            LOG.error("Got error message : " + e);
        }
    }

    /**
     * Sets the input and output paths for a {@link JobConf} and other generic configurations. Cleans the output path by
     * calling cleanOutputPath method
     * 
     * @param jobConf
     * @param inputPath
     * @param outputPath
     */
    public static void InitializeGenericConf(JobConf jobConf, String inputPath, String outputPath) {
        jobConf.setJarByClass(JobHelper.class);

        // Scan the provided input path for ARC files.
        if (inputPath != null) {
            FileInputFormat.addInputPath(jobConf, new Path(inputPath));
        }

        cleanOutputPath(outputPath);

        // Set the path where final output 'part' files will be saved.
        FileOutputFormat.setOutputPath(jobConf, new Path(outputPath));
        FileOutputFormat.setCompressOutput(jobConf, false);

        // Allow a 10% of failures, in case some files are corrupted, don't stop the whole process
        jobConf.setMaxMapTaskFailuresPercent(10);
    }

    /**
     * Configures a generic job with <Text,Text> outputs, an {@link IdentityReducer} and {@link TextInputFormat} (by
     * default) as input format.It also sets input and output paths for the passed {@link JobConf}. These configurations
     * can be overridden after calling this method
     * 
     * @param jobConf
     * @param inputPath
     * @param outputPath
     */
    public static void ConfigureTextualOutputJob(JobConf jobConf, String inputPath, String outputPath) {

        InitializeGenericConf(jobConf, inputPath, outputPath);

        // Set the output data types.
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(Text.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        // Set which OutputFormat class to use.
        jobConf.setOutputFormat(TextOutputFormat.class);
        jobConf.setReducerClass(IdentityReducer.class);
    }

    /**
     * Configures a generic job with <Text,LongWritable> outputs, a {@link LongSumReducer} and {@link TextInputFormat}
     * (by default) as input format.It also sets input and output paths for the passed {@link JobConf}. These
     * configurations can be overridden after calling this method
     * 
     * @param jobConf
     * @param inputPath
     * @param outputPath
     */
    public static void ConfigureNumericOutputJob(JobConf jobConf, String inputPath, String outputPath) {

        InitializeGenericConf(jobConf, inputPath, outputPath);

        // Set the output data types.
        jobConf.setMapOutputKeyClass(Text.class);
        jobConf.setMapOutputValueClass(LongWritable.class);
        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(LongWritable.class);

        // Set which OutputFormat class to use.
        jobConf.setOutputFormat(TextOutputFormat.class);

        // Set Reducer classes to use.
        jobConf.setCombinerClass(LongSumReducer.class);
        jobConf.setReducerClass(LongSumReducer.class);
    }

    /**
     * Reads the bucket name from the aws.properties file. A jUnit test is called before building to verify that this
     * value is not empty
     * 
     * @return the bucket name
     */
    public static String getBucketName() {
        List<String> paramsList = getListFromResource("/aws.properties");
        String bucketProperty = paramsList.get(2);
        if (bucketProperty.startsWith("s3Bucket=")) {
            bucketProperty = StringUtils.removeStart(bucketProperty, "s3Bucket=");
        }
        return bucketProperty;
    }
}
