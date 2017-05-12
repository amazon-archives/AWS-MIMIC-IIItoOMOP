package com.amazon.sparkbatchprocessor;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 * @author hoodrya
 */
public class Parser {

    public Configuration parse(String bucket, String key) throws IOException 
    {
        S3Object s3object = new AmazonS3Client().getObject(new GetObjectRequest(bucket, key));
        
        return parse(s3object.getObjectContent());
    }
    
    public Configuration parse(InputStream resource) throws IOException 
    {
        ObjectMapper mapper = new ObjectMapper();
        
        return mapper.readValue(resource, Configuration.class);
    }
}
