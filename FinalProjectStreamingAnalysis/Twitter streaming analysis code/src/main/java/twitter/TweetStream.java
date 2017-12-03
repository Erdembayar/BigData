package twitter;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;

import twitter4j.Status;

public class TweetStream {
    public static void main(String[] args) {
        final String consumerKey = "xxxxxxxxxxxxxxxxxxxxx";  //replace with your key. I deleted due to security risk.
        final String consumerSecret = "xxxxxxxxxxxxxxx"; //replace with your key. I deleted due to security risk.
        final String accessToken = "xxxxxxxxxxxxxxxxxx"; //replace with your key. I deleted due to security risk.
        final String accessTokenSecret = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"; //replace with your key. I deleted due to security risk.
        String localthost = "Localhost";
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitter").setMaster("local[4]").set("spark.driver.host",localthost).set("spark.driver.port","8080");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        Logger.getLogger("org").setLevel(Level.OFF);  //disable verbose output on console.
        Logger.getLogger("akka").setLevel(Level.OFF); //disable verbose output on console
        
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
        JavaDStream<String> statuses = twitterStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );


        statuses.print();
        jssc.start();
    }
}