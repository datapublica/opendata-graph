package com.datapublica.commoncrawl.indexing;

import java.io.IOException;
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
import com.datapublica.commoncrawl.utils.LanguageDetector;
import com.datapublica.commoncrawl.utils.MapHelper;

/**
 * Mapping class that detects the language of a text given as a value and then considers only the the French language
 * pages, then looks up for Open Data keywords in these pages. It outputs the full list of all the French websites with
 * their paths in Common Crawl AWS public dataset. In addition it outputs the ones among them which talk about open data
 */
public class FrenchWebIndexMapper extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

    // create a counter group for Mapper-specific statistics
    private static final String COUNTER_GROUP = "French Web Indexing Metrics";

    private static final String INVALID_URLS = "Invalid urls";

    private static final String EMPTY_PAGES = "Empty pages";

    private static final String UNDETECTED = "Undetected";

    private static final String OPENDATA_PAGES = "Open Data pages";

    private static final String FR = "fr";

    private static final String TAB = "\t";

    private static final String PATH = "PATH\t";

    private static final String OPENDATA = "OPENDATA\t";

    private static final String INPUT_FILE_PARAMETER = "map.input.file";

    private static final String PATH_PREFIX = "s3n://aws-publicdatasets/common-crawl/parse-output/segment/";

    private static final LongWritable one = new LongWritable(1);

    /**
     * List of open data words used by the French speaking communities
     */
    private static final String[] openDataWords = { "opendata", "open-data", "open data", "donnée publique",
                    "données publiques", "donnée ouverte", "données ouvertes", "jeux de données", "jeu de données",
                    "catalogue de données", "explorer les données", "donnée libérée", "données libérées" };

    private String filePath;

    public void configure(JobConf conf) {
        // Get the current input's file path
        filePath = conf.get(INPUT_FILE_PARAMETER);
    }

    public void map(Text key, Text value, OutputCollector<Text, LongWritable> output, Reporter reporter)
                    throws IOException {

        // Get the url
        String url = key.toString();

        // Get the value as the page text
        String pageText = value.toString();

        // If the page is empty than ignore (no title, no meta tags, and no content)
        if (StringUtils.isEmpty(pageText)) {
            reporter.incrCounter(COUNTER_GROUP, EMPTY_PAGES, 1);
            return;
        }

        try {
            // Gather the site from valid non-IP host name
            String website = MapHelper.getWebsiteFromUrl(url);

            if (website != null) {

                // Code for LangDetect Detection
                Detector detector = LanguageDetector.createDetector();
                detector.append(pageText);
                // throws an exception if the text doesn't contain enough features (less than 10 words)
                // caught bellow and reported as a metric
                String language = detector.detect();

                // output French sites only
                if (language.equals(FR)) {

                    // Clean prefix since it is redundant and it risks explose the output size
                    filePath = StringUtils.remove(filePath, PATH_PREFIX);

                    // Output index entry
                    output.collect(new Text(PATH + website + TAB + filePath), one);

                    // Open data keywords lookup
                    String text = value.toString();
                    for (String keyword : openDataWords) {
                        if (StringUtils.containsIgnoreCase(text, keyword)) {
                            output.collect(new Text(OPENDATA + website + TAB + filePath), one);
                            reporter.incrCounter(COUNTER_GROUP, OPENDATA_PAGES, 1);
                            break;
                        }
                    }
                }

            } else {
                reporter.incrCounter(COUNTER_GROUP, INVALID_URLS, 1);
            }

        } catch (LangDetectException ex) {
            reporter.incrCounter(COUNTER_GROUP, UNDETECTED, 1);
        }
    }
}
