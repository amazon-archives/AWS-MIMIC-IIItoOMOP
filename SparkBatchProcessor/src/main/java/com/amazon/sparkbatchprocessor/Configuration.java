package com.amazon.sparkbatchprocessor;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

@JsonAutoDetect
public class Configuration {

    @JsonProperty("dataSource")
    private String dataSource;
    
    @JsonProperty("destinationBucket")
    private String destinationBucket;
    
    @JsonProperty("sourceBucket")
    private String sourceBucket;
    
    @JsonProperty("sqlBucket")
    private String sqlBucket;
    
    @JsonProperty("fileSystem")
    private String fileSystem;
    
    @JsonProperty("mappings")
    private List<Mapping> mappings;
    
    public String getDataSource() { return dataSource; }
    
    public String getDestinationBucket() { return destinationBucket; }
    
    public String getSourceBucket() { return sourceBucket; }
    
    public String getSqlBucket() { return sqlBucket; }
    
    public String getFileSystem() { return fileSystem; }
    
    public List<Mapping> getMappings() { return mappings; }
    
    @Override
    public String toString() 
    {
        StringBuilder builder = new StringBuilder();
        
        builder.append("***** Configuration Details *****\n");
        builder.append("Data Source=" + dataSource + "\n");
        builder.append("Destination Bucket=" + destinationBucket + "\n");
        builder.append("Source Bucket=" + sourceBucket + "\n");
        builder.append("SQL Bucket=" + sqlBucket + "\n");
        builder.append("File System=" + fileSystem + "\n");
        builder.append("Mappings=" + mappings.toString() + "\n");
        builder.append("*****************************");

        return builder.toString();
    }
}
