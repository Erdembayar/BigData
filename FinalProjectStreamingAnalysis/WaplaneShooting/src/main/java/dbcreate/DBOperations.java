package dbcreate;
import java.awt.Point;
import java.io.IOException;
//import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;

public class DBOperations {

	private static final String TABLE_NAME = "WPLocations";
	private static final String CF_Point = "WPPoint";
	private static final String COL_PointX = "WPPointX";
	private static final String COL_PointY = "WPPointY";
	private static final String CF_TEAM = "TeamGroup";
	private static final String COL_TEAMID = "TeamID";

	public DBOperations() {
		try {
			generateHBaseTable();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void generateHBaseTable()  throws IOException {
		WarPlane jet = new WarPlane();
		List<Point[]> jetBodies = jet.getJets();
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			for(int i = 0; i < WarPlane.JETPOINTS; i++) {
				table.addFamily(new HColumnDescriptor(CF_Point + i)
						.setCompressionType(Algorithm.NONE));
			}
			table.addFamily(new HColumnDescriptor(CF_TEAM)
					.setCompressionType(Algorithm.NONE));
					
			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			System.out.println("jetBodies.size() : " + jetBodies.size());
			// for each airplane
			for (int i = 0; i < jetBodies.size(); i++) {
				Put record = new Put(Integer.toString(i).getBytes());
				// for each point of airplane add it to the corresponding columns
				for (int j = 0; j < WarPlane.JETPOINTS; j++) {
					record.addColumn((CF_Point + j).getBytes(), (COL_PointX + j).getBytes(), Integer.toString(jetBodies.get(i)[j].x).getBytes());
					record.addColumn((CF_Point + j).getBytes(), (COL_PointY + j).getBytes(), Integer.toString(jetBodies.get(i)[j].y).getBytes());
				}
				record.addColumn((CF_TEAM).getBytes(), (COL_TEAMID).getBytes(), Integer.toString(1).getBytes());
				connection.getTable(table.getTableName()).put(record);
			}			
			System.out.println(" Done!");
		}
	}
	
	public static void main(String... args) throws IOException {
		DBOperations dbo = new  DBOperations();
//		dbo.generateHBaseTable();
		/*
		WarPlane jet = new WarPlane();
		List<Point[]> jetBodies = jet.getJets();
		Configuration config = HBaseConfiguration.create();
		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			
			HTableDescriptor table = new HTableDescriptor(
					TableName.valueOf(TABLE_NAME));
			for(int i = 0; i < WarPlane.JETPOINTS; i++) {
				table.addFamily(new HColumnDescriptor(CF_Point + i)
						.setCompressionType(Algorithm.NONE));
			}
			table.addFamily(new HColumnDescriptor(CF_TEAM)
					.setCompressionType(Algorithm.NONE));
					
			System.out.print("Creating table.... ");

			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
			
			System.out.println("jetBodies.size() : " + jetBodies.size());
			// for each airplane
			for (int i = 0; i < jetBodies.size(); i++) {
				Put record = new Put(Integer.toString(i).getBytes());
				// for each point of airplane add it to the corresponding columns
				for (int j = 0; j < WarPlane.JETPOINTS; j++) {
					record.addColumn((CF_Point + j).getBytes(), (COL_PointX + j).getBytes(), Integer.toString(jetBodies.get(i)[j].x).getBytes());
					record.addColumn((CF_Point + j).getBytes(), (COL_PointY + j).getBytes(), Integer.toString(jetBodies.get(i)[j].y).getBytes());
				}
				record.addColumn((CF_TEAM).getBytes(), (COL_TEAMID).getBytes(), Integer.toString(1).getBytes());
				connection.getTable(table.getTableName()).put(record);
			}
			
			System.out.println(" Done!");
		}*/
	}
}
