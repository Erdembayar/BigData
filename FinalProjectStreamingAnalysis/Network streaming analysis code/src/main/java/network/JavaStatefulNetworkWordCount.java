package network;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;


public class JavaStatefulNetworkWordCount {
  private static final Pattern SPACE = Pattern.compile(" ");
  private static final int DURATION_SECONDS= 10;
  
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
    
    SparkConf sparkConf = new SparkConf().setAppName("StatefulNetworkWordCount").setMaster("local[4]");//.set("spark.driver.host",host).set("spark.driver.port","8080");
    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(DURATION_SECONDS));
  //  ssc.checkpoint(".");  //Needed for statefulness
    ssc.checkpoint("/tmp");  //Needed for statefulness

    // Initial state RDD input to mapWithState
    @SuppressWarnings("unchecked")
    List<Tuple2<String, Integer>> tuples =
        Arrays.asList(new Tuple2<>("hello", 1), new Tuple2<>("world", 1));
    JavaPairRDD<String, Integer> initialRDD = ssc.sparkContext().parallelizePairs(tuples);

    JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
    		host , Integer.parseInt(port), StorageLevels.MEMORY_AND_DISK_SER_2);

    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

    JavaPairDStream<String, Integer> wordsDstream = words.mapToPair(s -> new Tuple2<>(s, 1));

    // Update the cumulative count function
    Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>> mappingFunc =
        (word, one, state) -> {
          int sum = one.orElse(0) + (state.exists() ? state.get() : 0);
          Tuple2<String, Integer> output = new Tuple2<>(word, sum);
          state.update(sum);
          return output;
        };
        
    //https://stackoverflow.com/documentation/apache-spark/1924/stateful-operations-in-spark-streaming/6277/pairdstreamfunctions-mapwithstate#t=201608091620162599646
    // DStream made of get cumulative counts that get updated in every batch
    JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> stateDstream =
        wordsDstream.mapWithState(StateSpec.function(mappingFunc).initialState(initialRDD));

    stateDstream.print();
    ssc.start();
    ssc.awaitTermination();
  }
}