package com.amazon.sparkbatchprocessor;

import com.amazonaws.regions.Region;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;

/**
 *
 * @author hoodrya
 */
public class Loader {
    
    private SNSManager sns;
    private Parser parser = new Parser();
    private AmazonS3 client = new AmazonS3Client();
    private SparkSession spark;
    private Dataset<Row> df;
    Configuration configuration;
        
    public Loader(Region region) throws ClassNotFoundException, SQLException
    {
        spark = SparkSession.builder().appName("Spark batch loader").getOrCreate();
        sns = new SNSManager(region);
    }
    
    public Loader(String app, String master, String warehouse) throws ClassNotFoundException, SQLException
    {
        spark = SparkSession.builder().appName(app).config("spark.master", master).config("spark.sql.warehouse.dir", warehouse).getOrCreate();
    }
    
    public SparkSession getSparkSession() { return spark; }
    
    
    public void setConfiguration(Configuration configuration) 
    { 
        this.configuration = configuration; 
    }
    
    public void setDf(Dataset df) 
    { 
        this.df = df; 
    }
    
    public void loadSourceFiles(Mapping mapping)
    {
        for(String file : mapping.getSourceFiles())
        {
            String table = file.contains("/") ? file.substring(0, file.lastIndexOf("/")): file.substring(0, file.lastIndexOf("."));
            Dataset<Row> frame = spark.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("nullValue", "").option("treatEmptyValuesAsNulls","true").load(configuration.getFileSystem() + "://" + configuration.getSourceBucket() + "/" + file);
            frame.createOrReplaceTempView(table.replace("/", "_").replace("-", "_"));
        }
    }
    
    public Dataset<Row> loadDf(InputStream stream) throws IOException
    {
        return spark.sql(IOUtils.toString(stream, "UTF-8"));        
    }
    
    public void teardown(String stack, Region region) throws SQLException
    {
        CloudFormationManager cf = new CloudFormationManager(region);
        
        cf.terminateStack(stack);
        spark.stop();
    }
    
    public void load(Configuration configuration)
    {
        String destination = "";
        
        setConfiguration(configuration);
        
        try {
            for(Mapping mapping : configuration.getMappings())
            {
                destination = mapping.getSqlFile().contains("/") ? mapping.getSqlFile().substring(mapping.getSqlFile().lastIndexOf("/") + 1, mapping.getSqlFile().length()).replace(".sql", "") : mapping.getSqlFile().replace(".sql", "");
                
                loadSourceFiles(mapping);
                
                df = loadDf(client.getObject(new GetObjectRequest(configuration.getSqlBucket(), mapping.getSqlFile())).getObjectContent());
                
                if(mapping.getOverflowColumns().equals(new ArrayList<String>())) { write(destination); }
                else { write(destination, mapping.getOverflowColumns()); }
                
                sns.publishSuccess("SparkBatchLoaderTableCompletion", destination);
            }
        } 
        catch (IOException ex) { sns.publishFailure("SparkBatchLoaderTableFailure", destination, ex.toString()); }
    }
    
    public void write(String destination)
    {
        df.write().format("com.databricks.spark.csv").option("codec", "org.apache.hadoop.io.compress.GzipCodec").option("header", "true").mode("overwrite").save(configuration.getFileSystem() + "://" + configuration.getDestinationBucket() + "/stage/" + destination + ".csv");
    }
    
    public void write(String destination, List<String> overflowColumns) throws IOException 
    {
        String prefix = configuration.getFileSystem() + "://" + configuration.getDestinationBucket() + "/stage";
        String suffix = destination + ".csv"; 
        
        List<Column> columns = new ArrayList<Column>();
        List<String> strings = new ArrayList<String>();
        
        addMetadata(destination);
        materializeUUID(prefix, suffix);
        
        strings.add("file_location");
        columns.add(col("overlflow_column_uuid_lookup"));
        columns.add(col("file_location"));
        
        for(String column: overflowColumns) { columns.add(col(column)); strings.add(column); }
        
        df.select(JavaConverters.asScalaBufferConverter(columns).asScala()).write().format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").mode("overwrite").save(prefix + "/column_overflow/" + suffix);
        
        df.drop(JavaConverters.asScalaBufferConverter(strings).asScala()).write().format("com.databricks.spark.csv").option("header", "true").option("codec", "org.apache.hadoop.io.compress.GzipCodec").mode("overwrite").save(prefix + "/" + suffix);
    
        //clean up temp file
        new File(prefix + "/temp/" + suffix).delete();
    }
    
    public void addMetadata(String destination)
    {
        spark.udf().register("generateUUID", (String string) -> UUID.randomUUID().toString(), DataTypes.StringType);
        
        df = df.withColumn("overlflow_column_uuid_lookup", callUDF("generateUUID", lit("dummyvalue")));
        df = df.withColumn("file_location", lit(configuration.getFileSystem() + "://" + configuration.getDestinationBucket() + "/" + destination + ".csv"));
        df = df.withColumn("overflow_file_location", lit(configuration.getFileSystem() + "://" + configuration.getDestinationBucket() + "/column_overflow/" + destination + ".csv"));   
    }
    
    public void materializeUUID(String prefix, String suffix) throws IOException
    {
        String path = prefix + "/temp/" + suffix;
        
        //first write the dataframe to disk and read it back in to ensure that UUID's are fixed
        df.write().format("com.databricks.spark.csv").option("codec", "org.apache.hadoop.io.compress.GzipCodec").option("header", "true").mode("overwrite").save(path);
        
        df = spark.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "false").option("nullValue", "").option("treatEmptyValuesAsNulls","true").load(path);   
    }
}
