package dbcreate;

import java.awt.Point;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//enum JetBody { point0, point1, point2, point3, point4, point5, point6, point7, point8 };
enum JetDirection {up, right, down, left};
//enum CoordPoints { one, two, tree, four, five, six, seven, eight, nine, ten};

public class WarPlane {	
	static int BOARDSIZE = 10;
	static int JETPOINTS = 8;
	//private static int JETCOUNT = 1;
	// board: x [1, 10], y [1, 10] 
	// jet: should be defined by 8 points later, saving every points to the dataset helps to calculate probability
	// direction: jet head direction which is up / right / down / left. (it is intended for later use)
	// version table: Columns are id, teamid, point1, point2, point3,..., point10 (), direction
	// getPoints(headPoints, direction)
	
	//private char[][] board = new char[BOARDSIZE][BOARDSIZE];
	private int[][] board = new int[BOARDSIZE][BOARDSIZE];
	// Later jets should be inserted into array or list
	List<Point[]> jets = new ArrayList<Point[]>();
	
	public WarPlane() {
		for (int i = 0 ; i < board.length ; i++)
			for (int j = 0; j < board[i].length; j++)
				board[i][j] = ' ';
		
		for (int i = 0; i < WarPlane.BOARDSIZE; i++)
			for (int j = 0; j < WarPlane.BOARDSIZE; j++) {
				generateJet(new Point(j,i), JetDirection.up);
				generateJet(new Point(j,i), JetDirection.right);
				generateJet(new Point(j,i), JetDirection.down);
				generateJet(new Point(j,i), JetDirection.left);
			}
	}
	
	private Point[] getJetUp(Point headPoint) {
		Point[] jet = new Point[JETPOINTS];
		jet[0] = headPoint;
		jet[1] = new Point(headPoint.x - 1, headPoint.y + 1);
		jet[2] = new Point(headPoint.x, headPoint.y + 1);
		jet[3] = new Point(headPoint.x + 1, headPoint.y + 1);
		jet[4] = new Point(headPoint.x, headPoint.y + 2);
		jet[5] = new Point(headPoint.x - 1, headPoint.y + 3);
		jet[6] = new Point(headPoint.x, headPoint.y + 3);
		jet[7] = new Point(headPoint.x + 1, headPoint.y + 3);		
		return jet;
	}
	
	private Point[] getJetRight(Point headPoint) {
		Point[] jet = new Point[JETPOINTS];
		jet[0] = headPoint;
		jet[1] = new Point(headPoint.x - 1, headPoint.y - 1);
		jet[2] = new Point(headPoint.x - 1, headPoint.y);
		jet[3] = new Point(headPoint.x - 1, headPoint.y + 1);
		jet[4] = new Point(headPoint.x - 2, headPoint.y);
		jet[5] = new Point(headPoint.x - 3, headPoint.y - 1);
		jet[6] = new Point(headPoint.x - 3, headPoint.y);
		jet[7] = new Point(headPoint.x - 3, headPoint.y + 1);		
		return jet;
	}
	
	private Point[] getJetDown(Point headPoint) {
		Point[] jet = new Point[JETPOINTS];
		jet[0] = headPoint;
		jet[1] = new Point(headPoint.x + 1, headPoint.y - 1);
		jet[2] = new Point(headPoint.x, headPoint.y - 1);
		jet[3] = new Point(headPoint.x - 1, headPoint.y - 1);
		jet[4] = new Point(headPoint.x, headPoint.y - 2);
		jet[5] = new Point(headPoint.x + 1, headPoint.y - 3);
		jet[6] = new Point(headPoint.x, headPoint.y - 3);
		jet[7] = new Point(headPoint.x - 1, headPoint.y - 3);		
		return jet;
	}
	
	private Point[] getJetLeft(Point headPoint) {
		Point[] jet = new Point[JETPOINTS];
		jet[0] = headPoint;
		jet[1] = new Point(headPoint.x + 1, headPoint.y + 1);
		jet[2] = new Point(headPoint.x + 1, headPoint.y);
		jet[3] = new Point(headPoint.x + 1, headPoint.y - 1);
		jet[4] = new Point(headPoint.x + 2, headPoint.y);
		jet[5] = new Point(headPoint.x + 3, headPoint.y + 1);
		jet[6] = new Point(headPoint.x + 3, headPoint.y);
		jet[7] = new Point(headPoint.x + 3, headPoint.y - 1);		
		return jet;
	}
	
	public void generateJet(Point headPoint, JetDirection direction) {
		Point[] jet = new Point[JETPOINTS];
		switch(direction) {
		case up: 
			jet = getJetUp(headPoint);			
			break;
		case right:
			jet = getJetRight(headPoint);
			break;
		case down:
			jet = getJetDown(headPoint);
			break;
		case left:
			jet = getJetLeft(headPoint);
			break;
		}
		if(!isJetConflicting(jet))
			jets.add(jet);
	}
	
	public boolean isJetConflicting(Point[] jet) {
		for (int i = 0; i < jet.length; i++) {
			if (jet[i].x < 0 || jet[i].x >= BOARDSIZE)
				return true;
			if (jet[i].y < 0 || jet[i].y >= BOARDSIZE)
				return true;
		}
		return false;
	}
	
	public void drawJetOnBoard(List<Point[]> jets) {
		for(Point[] jet: jets) {
			for(int i = 0; i < jet.length; i++) {
				board[jet[i].y][jet[i].x] = board[jet[i].y][jet[i].x] + 1;
			}
			System.out.println("Output");
			for(int i = 0; i < board.length; i++) {
				System.out.println(Arrays.toString(board[i]));
			}
		}
	}
	
	public int[][] getBoard() {
		return board;
	}
	
	public List<Point[]> getJets() {
		return jets;
	}

	public static void main(String[] args) {
		List<Point[]> jetBody = null;
		WarPlane jet = new WarPlane();		
		jetBody = jet.getJets();		
		if (null == jetBody)
			System.out.println("Invalid jet");
		else
			jet.drawJetOnBoard(jet.jets);
		System.out.println("Permutations are: " + jet.jets.size());

	}

}
