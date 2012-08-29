/*
 * Copyright (C) by Data Publica, All Rights Reserved.
 */
package com.datapublica.commoncrawl.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class JobHelperTests {

    @Test
    public void TestGetListFromResource() throws IOException {

        Set<String> openDataSites = new HashSet<String>(JobHelper.getListFromResource("opendata-sites.txt"));

        assertNotNull(openDataSites);

    }

    @Test
    public void TestGetWebsiteFromUrl() throws IOException {

        String url = "https://console.aws.amazon.com/s3/home?#";

        System.out.print(MapHelper.getWebsiteFromUrl(url));

        // assertNotNull(openDataSites);

    }

}
