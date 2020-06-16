package com.aws.s3;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;

public class S3ClientOperations {

    private static S3Client s3;

    public static void main(String[] args) throws IOException {
        System.out.println("Creating the s3 client :");

        Region region = Region.AP_SOUTH_1;
        s3 = S3Client.builder().region(region).build();
        // the access id and access key are not provided in the code, instead we use
        // ProfileCredentialsProvider to load the credentials for aws credentials file(~/aws/credentials).

        String bucket = "bucket" + System.currentTimeMillis();
        String key = "key";

        S3BucketOperations bucketOperations = new S3BucketOperations();
        S3ObjectOperations objectOperations = new S3ObjectOperations();

        bucketOperations.createBucket(s3, bucket, region);
        System.out.println("bucket created successfully : " + bucket);

        System.out.println("list of all buckets : ");
        bucketOperations.listBucket(s3);

        System.out.println("uploading objects into bucket : ");
        objectOperations.uploadObject(s3, bucket, key);

        System.out.println("list of objects in the bucket : ");
        String objectKey = objectOperations.listObject(s3, bucket);

        objectOperations.getObject_(s3, bucket, objectKey);

        System.out.println("deleting the object from bucket : ");
        objectOperations.deleteObject_(s3, bucket, objectKey);

        System.out.println("deleting the bucket");
        bucketOperations.deleteBucket_(s3, bucket);
    }


}
