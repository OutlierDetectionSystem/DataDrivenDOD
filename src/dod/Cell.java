package dod;

import java.util.ArrayList;
import java.util.List;

/**
 * Cell for cell based algorithm
 * 
 * @author jzhu
 * @version May 13, 2013
 */
public class Cell {

	public int count;				// number of objects in this cell
	public int color;				// color of this cell; 2 -> red, 1 -> pink, 0 -> white
	public List<Tuple> points;		// points inside this cell

	public Cell() {
		count = 0;
		color = 0;
		points = new ArrayList<Tuple>();
	}

	public void addTuple(int num_dim, float[] point, long pointId) {
		this.points.add(new Tuple(num_dim, point, pointId));
		this.count ++;
	}

	public void print() {
		System.out.println(" count: " + count);
		System.out.println(" color: " + color);
		for(int i = 0; i < points.size(); i++){
			System.out.println(" Point " + i + ":");
			points.get(i).print();
		}
	}

}

/**
 * Tuple for a list of numbers
 * 
 * @author jzhu
 * @version May 14, 2013
 *
 */
class Tuple {

	public List<Float> elements;	// one element per dimension/axes
	public long pointId;  // id of the point
	
	public Tuple(int num_dim, float[] point, long pointId) {
		this.pointId = pointId;
		elements = new ArrayList<Float>();
		for(int i = 0; i < num_dim; i++){
			elements.add(point[i]);
		}
	}

	public void print() {
		if(elements.size()>0){
			System.out.print("  ");
		}
		for(int i = 0; i < elements.size(); i++){
			System.out.print(elements.get(i) + ",");
		}
		System.out.println("");
	}

	public static float distBetweenTwoTuples(Tuple t1, Tuple t2){
		float dist = 0;
		// assume t1 and t2 have the same number of dimensions; otherwise it does not make sense
		for(int i = 0; i < t1.elements.size(); i++){
			dist += Math.pow(t1.elements.get(i) - t2.elements.get(i), 2);
		}
		dist = (float)Math.sqrt(dist);
		return dist;
	}

}
