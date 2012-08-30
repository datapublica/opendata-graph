package com.datapublica.commoncrawl.utils;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;

import com.google.common.net.InternetDomainName;

public class MapHelper {

    /**
     * Gets the website name from a given url (not the top private domain name but the full site with all its
     * subdomains). The url must have a valid host name (not an IP) and a valid public suffix
     * 
     * @param url the url to extract website name from
     * @return the website name if the url is valid otherwise it returns null
     */
    public static String getWebsiteFromUrl(String url) {
        // Gather the host name
        URI uri;
        try {
            uri = new URI(url);
            String host = StringUtils.trimToNull(uri.getHost());

            // Skip empty host names
            if (host != null) {
                // Gather the domain object from a valid non-IP host name
                if (InternetDomainName.isValid(host)) {
                    return InternetDomainName.from(host).name();
                }
            }
        } catch (URISyntaxException e) {
            return null;
        }
        return null;

    }

}
