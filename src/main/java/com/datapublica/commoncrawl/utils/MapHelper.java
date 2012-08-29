/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.utils;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;

import com.google.common.net.InternetDomainName;

public class MapHelper {

    public static String getWebsiteFromUrl(String url) {
        // Gather the host name
        URI uri;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            return null;
        }

        String host = StringUtils.trimToNull(uri.getHost());

        // Skip empty host names
        if (host == null) {
            return null;
        }

        // Gather the domain object from a valid non-IP host name
        if (InternetDomainName.isValid(host)) {
            return InternetDomainName.from(host).name();

        } else {
            return null;
        }

    }

}
