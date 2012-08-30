package com.datapublica.commoncrawl.utils;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;

public class ProjectDeployer {

    private final static Log LOG = LogFactory.getLog(ProjectDeployer.class);

    /**
     * Deploys the projet jar on amazon S3 to be executed manually in EMR console
     * 
     * @param args {awsAccessId} {awsSecretKey} {bucketName} {jarPath}
     * @throws S3ServiceException
     * @throws NoSuchAlgorithmException
     */
    public static void main(String[] args) throws S3ServiceException, NoSuchAlgorithmException {

        AWSCredentials credentials = new AWSCredentials(args[0], args[1]);
        S3Service s3 = new RestS3Service(credentials);

        LOG.info("===========================================");
        LOG.info("Deploying the project JAR on Amazon S3");
        LOG.info("===========================================\n");

        // Verify connection and the existence of the common crawl project bucket
        S3Bucket dpCommonCrawlBucket = s3.getBucket(args[2]);
        if (dpCommonCrawlBucket.getName().equals(args[2])) {
            LOG.info("Connected successfully to the S3 Bucket : " + args[2]);
        }

        File fileData = new File(args[3]);
        try {

            // Create an S3Object based on a file, with Content-Length set automatically and
            // Content-Type set based on the file's extension (using the Mimetypes utility class)
            S3Object fileObject = new S3Object(fileData);

            // Calculate the hash value for verification purposes
            String localFileHash = fileObject.getMd5HashAsHex();

            // Upload the file
            LOG.info("Uploading file : " + fileObject.getKey() + " | Size = " + fileObject.getContentLength() / 1024
                            + " Ko");
            s3.putObject(dpCommonCrawlBucket, fileObject);

            // Reconnect to the server to verify that the file is uploaded successfully
            // TODO : system error
            try {
                // Calculate the hash value for the uploaded file in the S3 bucket
                String remoteFileHash = s3.getObjectDetails(args[2], fileObject.getKey()).getMd5HashAsHex();

                // Check if the file is uploaded successfully
                if (remoteFileHash.equals(localFileHash)) {
                    LOG.info("File uploaded successfully : " + fileObject.getKey());
                } else {
                    LOG.warn("File uploaded but hash verification failed : " + localFileHash + " <> " + remoteFileHash);
                }
            } catch (ServiceException s3e) {
                LOG.error("Could verify the upload. Got the error from S3 server :");
                LOG.error("Error Message: " + s3e.getErrorMessage());
                LOG.error("S3 Error Code: " + s3e.getErrorCode());
                LOG.error("Request ID:    " + s3e.getErrorRequestId());
            }

        } catch (IOException e) {
            LOG.error("Could not access file : " + fileData.getName());
            e.printStackTrace();
        }

    }
}
