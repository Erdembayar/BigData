package dbcreate;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;

public class SparkSQLTest {

	public static void main(String[] args) {
		final SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("HiveConnector");
	    final JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
	    SQLContext sqlContext = new HiveContext(sparkContext);

	    //DataFrame df = sqlContext.sql("SELECT * FROM test_hive_table1");
	    DataFrame df = sqlContext.sql("show databases");
	    df.show();
	    df.count();

	}

}
