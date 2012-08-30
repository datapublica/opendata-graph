package com.datapublica.commoncrawl.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

public class mandatoryElementsTests {

    /**
     * Tests if the aws.properties file located in src/main/java is filled with the AWS credentials and the bucket name
     * 
     * @throws IOException
     */
    @Test
    public void TestAWScredentialsPresence() throws IOException {

        List<String> paramsList = JobHelper.getListFromResource("/aws.properties");
        for (int i = 0; i < paramsList.size(); i++) {
            String bucketProperty = paramsList.get(i);
            bucketProperty = StringUtils.substringAfter(bucketProperty, "=");
            assertFalse(bucketProperty.isEmpty());
        }
    }

}
