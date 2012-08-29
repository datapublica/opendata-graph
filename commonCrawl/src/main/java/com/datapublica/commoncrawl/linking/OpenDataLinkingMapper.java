/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
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
 * Mapping class that produces the normalized domain name and a count of '1' for every successfully retrieved URL in the
 * Common Crawl corpus. Actually we consider only the url passed as a Key for the map method. Further we might process
 * the json metadata contained in Value
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

        // key & value are "Text" right now ...
        String url = key.toString();
        String json = value.toString();

        reporter.incrCounter(COUNTER_GROUP, "Total pages", 1);

        // See if the page has a successful HTTP code
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
            reporter.incrCounter(COUNTER_GROUP, "json error", 1);
            return;
        }

        String website = MapHelper.getWebsiteFromUrl(url);
        if (website != null) {
            reporter.incrCounter(COUNTER_GROUP, "Total treated pages", 1);

            if (RunOpenDataLinking.openDataSites.contains(website)) {
                reporter.incrCounter(COUNTER_GROUP, "Total opendata pages", 1);

                JsonElement content = null;

                if (jsonObj.has(CONTENT)) {
                    content = jsonObj.get(CONTENT);

                    if (content.getAsJsonObject().has(LINKS)) {

                        Iterator<JsonElement> linksIterator = content.getAsJsonObject().getAsJsonArray(LINKS)
                                        .iterator();

                        List<String> rawSitesList = new ArrayList<String>();

                        while (linksIterator.hasNext()) {

                            JsonObject link = linksIterator.next().getAsJsonObject();
                            String linkType = link.get(TYPE).getAsString();

                            if (linkType.equals(A_LINK_TYPE)) {
                                String linkSite = MapHelper.getWebsiteFromUrl(link.get(HREF).getAsString());
                                rawSitesList.add(linkSite);
                            }
                        }

                        Set<String> unduplicateLinks = new HashSet<String>(rawSitesList);
                        unduplicateLinks.remove(website);
                        for (String externalSite : unduplicateLinks) {
                            if (RunOpenDataLinking.openDataSites.contains(externalSite)) {
                                output.collect(new Text(website), new Text(externalSite));
                            }
                        }
                    }
                }
            }
        }
    }

}
