package twitter;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationContext;

public class StreamingTweetsSimple {

    public static void main(String[] args) {

        // Twitter4J
        final String consumerKey = "xxxxxxxxxxxxxx";   //replace with your key. I deleted due to security risk.
        final String consumerSecret = "xxxxxxxxxx";   //replace with your key. I deleted due to security risk.
        final String accessToken = "x-xxxxxxxx"; //replace with your key. I deleted due to security risk.
        final String accessTokenSecret = "xxxxxxxxxxxxxxxx"; //replace with your key. I deleted due to security risk.

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        
        Logger.getLogger("org").setLevel(Level.OFF);  //disable verbose output on console.
        Logger.getLogger("akka").setLevel(Level.OFF); //disable verbose output on console
        
        // Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("Tweets Money")
                .setMaster("local[2]");
        JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

        // basic stats on tweets
        String[] filters = { "#Money" };
        TwitterUtils.createStream(sc,  filters)
                .flatMap(s -> Arrays.asList(s.getHashtagEntities()))
                .map(h -> h.getText().toLowerCase())
                .filter(h -> !h.equals("money"))
                .countByValue()
                .print();

        sc.start();
        sc.awaitTermination();
    }
}