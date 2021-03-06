package network;

import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.base.Optional;

import org.apache.log4j.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;

import scala.Tuple2;

import java.util.*;
public final class JavaNetworkWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final int DURATION_SECONDS= 10;
	private static final String TABLE_NAME = "networkWordCount";
	private static final String CF_DEFAULT = "words";
	private static int counter=1;
  public static void main(String[] args) throws Exception {
	  String host="";
	  String port="";
    if (args.length < 2) {
    	host= "192.168.56.1"; //you need to change your own server IP
    	port= "3000"; //port you used
    }
    else{
    	host = args[0];
    	port = args[1];
    }

    Logger.getLogger("org").setLevel(Level.OFF);  //disable verbose output on console.
    Logger.getLogger("akka").setLevel(Level.OFF); //disable verbose output on console

    String localthost = "Localhost";
    SparkConf config = new SparkConf().setAppName("NetworkWordCount").setMaster("local[4]").set("spark.driver.host",localthost).set("spark.driver.port","8080");

    JavaStreamingContext ssc = new JavaStreamingContext(config, Durations.seconds(DURATION_SECONDS)) ;

    // Create a JavaReceiverInputDStream on target ip:port and count the
    // words in input stream of \n delimited text (eg. generated by 'nc')
    // Note that no duplication in storage level only for running locally.
    // Replication necessary in distributed scenario for fault tolerance.
    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
            host, Integer.parseInt(port), StorageLevels.MEMORY_AND_DISK_SER);
    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator()); //
    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
        .reduceByKey((i1, i2) -> i1 + i2);

//    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//    Date date = new Date();
//    System.out.println(dateFormat.format(date)); //2016/11/16 12:08:43
//    
    HashMap<String,Integer> hm=new HashMap<String,Integer>();  
    Configuration Hconfig = HBaseConfiguration.create();

    
	wordCounts.foreachRDD(
            new VoidFunction<JavaPairRDD<String, Integer>>() {

				@Override
				public void call(JavaPairRDD<String, Integer> t)
						throws Exception {
                  System.out.println("\n===================================");
                  hm.clear();
                 // System.out.println("New Events for " + time + " batch:");
                  for (Tuple2<String, Integer> tuple : t.collect()) {
                      System.out.println("Dumping: "+tuple._1 + ": " + tuple._2);
                      hm.put(tuple._1,tuple._2);  
                  	
                	try (Connection connection = ConnectionFactory.createConnection(Hconfig);
                			Admin admin = connection.getAdmin()) {
                		HTableDescriptor table = new HTableDescriptor(
                				TableName.valueOf(TABLE_NAME));
                		table.addFamily(new HColumnDescriptor(CF_DEFAULT)
                				.setCompressionType(Algorithm.NONE));

                		//System.out.print("Creating table.... ");
                		System.out.print("Creating table.... " + table.getNameAsString());

                		//System.out.println(admin);
//                		if (admin.tableExists(table.getTableName())) {
//                			admin.disableTable(table.getTableName());
//                			admin.deleteTable(table.getTableName());
//                		}
//                		admin.createTable(table);
                		if (!admin.tableExists(table.getTableName())) {
                			admin.createTable(table);
                		}
                		
                		
                		Table tableUser = connection.getTable(TableName.valueOf(TABLE_NAME));

                		 for(Map.Entry m:hm.entrySet()){  
                			   System.out.println("HashMap:"+m.getKey()+" "+m.getValue());  
                		        Put put = new Put(Bytes.toBytes(counter));
                		        counter++;
                		        put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("word"),
                		                Bytes.toBytes(m.getKey().toString()));
                		       // counter++;
                		        put.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("count"),
                		                Bytes.toBytes( m.getValue().toString()));

                		        tableUser.put(put);
                			  }  
                		 

                			
                		System.out.println("\n Done!");
                	}
                	
                  }
					
				}


   });

	//System.out.println("zzzzzzzzzzzz");
//	 for(Map.Entry m:hm.entrySet()){  
//		   System.out.println("HashMap111:"+m.getKey()+" "+m.getValue());  
//
//		  }  
//	 


    wordCounts.print();
    ssc.start();
    ssc.awaitTermination();
  }
}