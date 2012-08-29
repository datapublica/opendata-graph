/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.indexing;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.LangDetectException;
import com.google.common.net.InternetDomainName;

import com.datapublica.commoncrawl.utils.LanguageDetector;

/**
 * Mapping class that produces the normalized domain name and a count of '1' for every successfully retrieved URL in the
 * Common Crawl corpus. Actually we consider only the url passed as a Key for the map method. Further we might process
 * the json metadata contained in Value
 */
public class FrenchWebIndexingMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

    // create a counter group for Mapper-specific statistics
    private static final String COUNTER_GROUP = "Language detection statistics";

    private static final String INVALID_URLS = "Invalid urls";

    private static final String EMPTY_PAGES = "Empty pages";

    private static final String UNDETECTED = "Undetected";

    private static final String OPENDATA_PAGES = "Open Data pages";

    private static final String FR = "fr";

    private static final String TAB = "\t";

    private static final String PATH = "PATH\t";

    private static final String OPENDATA = "OPENDATA\t";

    private static final String INPUT_FILE_PARAMETER = "map.input.file";

    private static final LongWritable one = new LongWritable(1);

    private static final String[] openDataWords = { "opendata", "open-data", "open data", "donnée publique",
                    "données publiques", "donnée ouverte", "données ouvertes", "jeux de données", "jeu de données",
                    "catalogue de données", "explorer les données", "donnée libérée", "données libérées" };

    private String filePath;

    public void configure(JobConf conf) {
        filePath = conf.get(INPUT_FILE_PARAMETER);
    }

    public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Use the URLs to gather TLD-language statistics
        String url = key.toString();

        // Get the value as the text
        String pageText = value.toString();

        // If the page is empty than ignore (no title, no meta tags, and no content)
        if (StringUtils.isEmpty(pageText)) {
            reporter.incrCounter(COUNTER_GROUP, EMPTY_PAGES, 1);
            return;
        }

        try {

            // Gather the host name
            URI uri = new URI(url);

            String host = StringUtils.trimToNull(uri.getHost());

            // Skip empty host names
            if (host == null) {
                throw new URISyntaxException(url, INVALID_URLS);
            }

            // Gather the domain object from a valid non-IP host name

            if (InternetDomainName.isValid(host)) {
                InternetDomainName domainObj = InternetDomainName.from(host);

                // Code for LangDetect Detection
                Detector detector = LanguageDetector.createDetector();
                detector.append(pageText);
                // throws an exception if the text doesn't contain enough features (less than 10 words)
                // caught bellow and reported as a metric
                String language = detector.detect();

                // output French domains
                if (language.equals(FR)) {

                    // Output index entry
                    output.collect(new Text(PATH + domainObj.name() + TAB + filePath), one);

                    // Open data lookup
                    String text = value.toString();
                    for (String keyword : openDataWords) {
                        if (StringUtils.containsIgnoreCase(text, keyword)) {
                            output.collect(new Text(OPENDATA + domainObj.name() + TAB + filePath), one);
                            reporter.incrCounter(COUNTER_GROUP, OPENDATA_PAGES, 1);
                            break;
                        }
                    }
                }

            } else {
                throw new URISyntaxException(url, INVALID_URLS);
            }

        } catch (URISyntaxException ex) {
            reporter.incrCounter(COUNTER_GROUP, INVALID_URLS, 1);
        } catch (LangDetectException ex) {
            reporter.incrCounter(COUNTER_GROUP, UNDETECTED, 1);
        }
    }
}
