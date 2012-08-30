package com.datapublica.commoncrawl.linking;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.datapublica.commoncrawl.utils.MapHelper;

/**
 * A mapper that looks into the metadata of a given url (and filters urls to take only the ones from the open data
 * sites). It extracts the list of all the websites in the external links of this page and takes only the websites in
 * the open data sites list. The output is the list of all the interlinks between the French open data sites. Pattern :
 * sourceOpenDataSite destinationOpenDataSite
 */
public class OpenDataLinkingMapper extends MapReduceBase implements Mapper<Text, Text, Text, Text> {

    // create a counter group for Mapper-specific statistics
    private static final String COUNTER_GROUP = "Opendata links stats";

    private static final String CONTENT = "content";

    private static final String LINKS = "links";

    private static final String TYPE = "type";

    private static final String A_LINK_TYPE = "a";

    private static final String HREF = "href";

    public void map(Text key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {

        // get the urls and their jon metadata as strings
        String url = key.toString();
        String json = value.toString();

        // Parse the metadata as Json
        JsonParser jsonParser = new JsonParser();
        JsonObject jsonObj = null;
        try {
            JsonElement parsedJson = jsonParser.parse(json);
            if (parsedJson.isJsonObject()) {
                jsonObj = parsedJson.getAsJsonObject();
            } else {
                return;
            }
        } catch (Exception e) {
            // It looks that some json of some records is corrupted at many levels and the parser used to throw
            // different exceptions so I generalized this to don't get my job failed at the last moment. FYI : total
            // json_error metric in the last execution over the whole corpus was 3433
            reporter.incrCounter(COUNTER_GROUP, "json error", 1);
            return;
        }

        // Get the website's name (not the top private domain name)
        String website = MapHelper.getWebsiteFromUrl(url);

        // Process only valid websites names
        if (website != null) {

            // Process only open data sites
            if (RunOpenDataLinking.openDataSites.contains(website)) {

                // Collect some stats
                reporter.incrCounter(COUNTER_GROUP, "Total opendata pages", 1);

                JsonElement content = null;

                // Get the content element
                if (jsonObj.has(CONTENT)) {
                    content = jsonObj.get(CONTENT);

                    // Get all the links
                    if (content.getAsJsonObject().has(LINKS)) {

                        Iterator<JsonElement> linksIterator = content.getAsJsonObject().getAsJsonArray(LINKS)
                                        .iterator();
                        List<String> rawSitesList = new ArrayList<String>();

                        // For each link
                        while (linksIterator.hasNext()) {

                            JsonObject link = linksIterator.next().getAsJsonObject();
                            String linkType = link.get(TYPE).getAsString();

                            // Take the "a" links only
                            if (linkType.equals(A_LINK_TYPE)) {

                                // Extract the website from the link
                                String linkSite = MapHelper.getWebsiteFromUrl(link.get(HREF).getAsString());
                                rawSitesList.add(linkSite);
                            }
                        }

                        // unduplicate the websites list
                        Set<String> unduplicateSites = new HashSet<String>(rawSitesList);

                        // Remove the current website from the list
                        unduplicateSites.remove(website);

                        for (String externalSite : unduplicateSites) {
                            // Take only the open data sites
                            if (RunOpenDataLinking.openDataSites.contains(externalSite)) {
                                // Pattern : sourceOpenDataSite destinationOpenDataSite
                                output.collect(new Text(website), new Text(externalSite));
                            }
                        }
                    }
                }
            }
        }
    }
}
