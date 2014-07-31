/**
 * 
 */
package com.emc.atmos.sync.util;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;

/**
 * @author cwikj
 *
 */
public class S3Utils {
    /**
     * The AWS Java SDK actually does a listBucket for this instead of a real HEAD.  This
     * can cause issues with Atmos since it's recursive by default.  Limit the scope of
     * the request for performance reasons.
     * @param amz
     * @param bucketName
     * @return
     */
    public static boolean doesBucketExist(AmazonS3 amz, String bucketName) {
        return amz.doesBucketExist(bucketName);
//        ListObjectsRequest lor = new ListObjectsRequest()
//            .withBucketName(bucketName)
//            .withDelimiter("/")
//            .withMaxKeys(1);
//        
//        try {
//            amz.listObjects(lor);
//            return true;
//        } catch(AmazonServiceException e) {
//            if(e.getStatusCode() == 404) {
//                return false;
//            } else {
//                throw e;
//            }
//        }
    }
}
