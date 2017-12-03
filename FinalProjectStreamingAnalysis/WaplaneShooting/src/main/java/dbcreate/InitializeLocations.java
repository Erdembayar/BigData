package dbcreate;
import java.awt.Point;
import java.io.IOException;
//import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;;

@SuppressWarnings("unused")
public class InitializeLocations {
	static String HIVE_TABLE = "WPLocations";
	static final String CREATE_WPLOCATIONS_TABLE 
			= "CREATE TABLE IF NOT EXISTS " + HIVE_TABLE 
			+ "(TeamID STRING, POINT0X INT, POINT0Y INT, "
			+ "POINT1X INT, POINT1Y INT, POINT2X INT, POINT2Y INT, "
			+ "POINT3X INT, POINT3Y INT, POINT4X INT, POINT4Y INT, "
			+ "POINT5X INT, POINT5Y INT, POINT6X INT, POINT6Y INT, "
			+ "POINT7X INT, POINT7Y INT)";
	
	static final String INSERT_INTO_TABLE 
		= "INSERT INTO TABLE WPLocations SELECT v.* from (SELECT \"%s\", "
		+ "%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d) v";
	
	static final String SELECT_GROUP_COUNT = "SELECT COUNT(POINT0X), COUNT(POINT0Y), "
			+ "COUNT(POINT1X), COUNT(POINT1Y), COUNT(POINT2X), COUNT(POINT2Y), "
			+ "COUNT(POINT3X), COUNT(POINT3Y), COUNT(POINT4X), COUNT(POINT4Y), "
			+ "COUNT(POINT5X), COUNT(POINT5Y), COUNT(POINT6X), COUNT(POINT6Y), "
			+ "COUNT(POINT7X), COUNT(POINT7Y) FROM " + HIVE_TABLE + " GROUP BY TeamID";
	
	static final String SELECT_ALL = "SELECT * FROM " + HIVE_TABLE;
	
	static final String SELECT_POINT = 
			"SELECT * FROM " + HIVE_TABLE + " WHERE (POINT0X = %d AND POINT0Y = %d) "
			+ "OR (POINT1X = %d AND POINT1Y = %d) OR (POINT2X = %d AND POINT2Y = %d) "
			+ "OR (POINT3X = %d AND POINT3Y = %d) OR (POINT4X = %d AND POINT4Y = %d) "
			+ "OR (POINT5X = %d AND POINT5Y = %d) OR (POINT6X = %d AND POINT6Y = %d) "
			+ "OR (POINT7X = %d AND POINT7Y = %d)";
	
	static final String SELECT_EXCEPT_POINT = 
			"SELECT * FROM " + HIVE_TABLE + " WHERE (POINT0X <> %d AND POINT0Y <> %d) "
					+ "OR (POINT1X <> %d AND POINT1Y <> %d) OR (POINT2X <> %d AND POINT2Y <> %d) "
					+ "OR (POINT3X <> %d AND POINT3Y <> %d) OR (POINT4X <> %d AND POINT4Y <> %d) "
					+ "OR (POINT5X <> %d AND POINT5Y <> %d) OR (POINT6X <> %d AND POINT6Y <> %d) "
					+ "OR (POINT7X <> %d AND POINT7Y <> %d)";

	static final String SELECT_COUNT_POINT = 
			"SELECT POINT0X, POINT0Y, COUNT(*) as Y FROM " + HIVE_TABLE + " GROUP BY POINT0X, POINT0Y order by Y desc";
	
	static final String DROP_TABLE 
			= "DROP TABLE "  + HIVE_TABLE;
	
	private DataFrame df ;
	private WarPlane jet ;
	private List<Point[]> jetBodies ;
	private SparkConf conf ;		
	private JavaSparkContext sc ;
	private SQLContext hiveContext ;
	
	public InitializeLocations() {
		try {
			jet = new WarPlane();
			jetBodies = jet.getJets();
			conf = new SparkConf().setAppName("HiveDB").setMaster("local");		
			sc = new JavaSparkContext(conf);
			hiveContext = new HiveContext(sc);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void generateHBaseTable()  throws IOException {
		hiveContext.sql(DROP_TABLE);
		hiveContext.sql(CREATE_WPLOCATIONS_TABLE);		
		System.out.println("jetBodies.size() : " + jetBodies.size());
		// for each airplane
		for (int i = 0; i < jetBodies.size(); i++) {
			//Put record = new Put(Integer.toString(i).getBytes());
			// for each point of airplane add it to the corresponding columns
			hiveContext.sql(String.format(INSERT_INTO_TABLE, "TeamKey01", 
					jetBodies.get(i)[0].x, jetBodies.get(i)[0].y,
					jetBodies.get(i)[1].x, jetBodies.get(i)[1].y,
					jetBodies.get(i)[2].x, jetBodies.get(i)[2].y,
					jetBodies.get(i)[3].x, jetBodies.get(i)[3].y,
					jetBodies.get(i)[4].x, jetBodies.get(i)[4].y,
					jetBodies.get(i)[5].x, jetBodies.get(i)[5].y,
					jetBodies.get(i)[6].x, jetBodies.get(i)[6].y,
					jetBodies.get(i)[7].x, jetBodies.get(i)[7].y ));
		}
	}
	
	public void executeQuery(String sqlText) {
		df = hiveContext.sql(sqlText);
		df.show();
		//sc.close();
	}
	
	public void executeQuery(String sqlText, Point p) {
		df = hiveContext.sql(String.format(sqlText, p.x, p.y, p.x, p.y, p.x, p.y, p.x, p.y, p.x, p.y, p.x, p.y, p.x, p.y, p.x, p.y));
		df.show();
		//sc.close();
	}
	
	public static void main(String... args) throws IOException {
		InitializeLocations dbo = new  InitializeLocations();
		dbo.executeQuery(DROP_TABLE);
		//dbo.generateHBaseTable();
		//dbo.executeQuery(SELECT_POINT);
		//dbo.executeQuery(SELECT_EXCEPT_POINT, new Point(5, 5 ));
		//dbo.executeQuery(SELECT_ALL);
	}
}
