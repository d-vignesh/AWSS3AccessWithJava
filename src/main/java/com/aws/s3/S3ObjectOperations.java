package com.aws.s3;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.File;
import java.util.List;
import java.util.ListIterator;

public class S3ObjectOperations {

    // Put object
    public void uploadObject(S3Client s3Client, String bucketName, String key) {
        try {
            File fp = new File("/home/vignesh/my_drive/Docker/kubernetes_example/edx_course_deployments.yml");

            s3Client.putObject(
                    PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                    RequestBody.fromFile(fp)
                    );
        }
        catch(S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }

    // list object
    public String listObject(S3Client s3Client, String bucketName) {
        try {
            ListObjectsRequest listObjectsReq = ListObjectsRequest
                    .builder()
                    .bucket(bucketName)
                    .build();

            ListObjectsResponse response = s3Client.listObjects(listObjectsReq);
            List<S3Object> objList = response.contents();
            String objectKey = "";

            for(ListIterator iterVals = objList.listIterator(); iterVals.hasNext(); ) {
                S3Object myValue = (S3Object) iterVals.next();
                objectKey = myValue.key();
                System.out.println("\n The name of the key is : " + myValue.key());
                System.out.println("\n The Object is " + calKb(myValue.size()) + " KBs");
                System.out.println("\n the owner is : " + myValue.owner());
            }
            return objectKey;
        }
        catch(S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
        return "";
    }

    private static long calKb(Long val) {
        return val/1024;
    }

    // get object
    public void getObject_(S3Client s3Client, String bucketName, String objectKey) {
        try {
            System.out.format("downloading %s from %s...\n", objectKey, bucketName);
            s3Client.getObject(
                    GetObjectRequest.builder().bucket(bucketName).key(objectKey).build(),
                    ResponseTransformer.toFile(new File("/home/vignesh/test.yml"))
            );
        }
        catch(S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
        }
    }

    // delete object
    public void deleteObject_(S3Client s3Client, String bucketName, String objectKey) {
        try {
            System.out.format("deleting %s from %s...\n", objectKey, bucketName);
            DeleteObjectRequest deleteReq = DeleteObjectRequest.builder().bucket(bucketName).key(objectKey).build();
            s3Client.deleteObject(deleteReq);
        }
        catch(S3Exception e) {
            System.err.println(e.awsErrorDetails().errorMessage());
            System.exit(1);
        }
    }
}
