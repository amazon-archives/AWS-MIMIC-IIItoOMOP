package com.amazon.sparkbatchprocessor;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author hoodrya
 */
@JsonAutoDetect
public class Mapping 
{
    @JsonProperty("sourceFiles")
    private List<String> sourceFiles;
    
    @JsonProperty("sqlFile")
    private String sqlFile;
    
    @JsonProperty("overflowColumns")
    private List<String> overflowColumns;
    
    public List<String> getSourceFiles() { return sourceFiles; }
    
    public String getSqlFile() { return sqlFile; }
    
    public List<String> getOverflowColumns() 
    {
        return overflowColumns == null ? new ArrayList<String>() : overflowColumns;
    }
    
    @Override
    public String toString() 
    {
        StringBuilder builder = new StringBuilder();
        
        builder.append("\n***** Mapping Details *****\n");
        builder.append("Source Files=" + sourceFiles.toString() + "\n");
        builder.append("SQL File=" + sqlFile + "\n");
        builder.append("Overflow Columns=" + getOverflowColumns().toString() + "\n");
        builder.append("*****************************");

        return builder.toString();
    }
}
