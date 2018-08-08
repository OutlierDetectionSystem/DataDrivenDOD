package dod;

import java.util.ArrayList;
import java.util.List;

/**
 * The whole cell structure containing cells used in cell based algorithm
 * 
 * @author jzhu
 * @version May 14, 2013
 */
public class CellStruct {

	public List<Cell>cells;					// cells
	private int num_dim;					// number of dimensions, equal to num_col in Cell_Based_Algo.java
	private int num_data;					// number of total data, equal to num_row in Cell_Based_Algo.java
	private List<Integer> numCellsPerDim;	// number of cells per dimension
	private int numCells;
	private float cell_size;				// size of a cell
	private float overlapLg;				// length of overlapped/extended area
	private List<Float> blkStartPosPerDim;

	/**
	 * Default constructor for a cell structure 
	 * @param num_dim
	 * @param num_data
	 * @param numCellsPerDim
	 * @param cell_size
	 * @param blk_index
	 * @param edgeLgList
	 * @param overlapLg
	 * @param blkStartPosPerDim 
	 */
	public CellStruct(int num_dim, int num_data, List<Integer> numCellsPerDim, float cell_size,float overlapLg, List<Float> blkStartPosPerDim) {
		cells = new ArrayList<Cell>();
		this.num_dim = num_dim;
		this.num_data = num_data;
		this.numCellsPerDim = numCellsPerDim;
		this.numCells = 1;
		this.cell_size = cell_size;
		this.overlapLg = overlapLg;
		this.blkStartPosPerDim = blkStartPosPerDim;
	}

	/**
	 * Create cells in this cell structure and map points into the correct cell
	 * @param points data read from the input file
	 */
	public void mapPointToCell(float[][] points, long [] pointIds) {
		// calculate number of cells
		for(int i = 0; i < num_dim; i++){
			numCells *= numCellsPerDim.get(i);
		}
		// create cells
		for(int i = 0; i < numCells; i++){
			this.cells.add(i, new Cell());
		}
		
		// map/add points into cells
		for(int i = 0; i < num_data; i++){
			// calculate which cell this point will be put in; nest_index -> flat_index
			int nest_index[] = new int[num_dim];
			int flat_index = 0;
			for(int j = 0; j < num_dim; j ++){
				//				nest_index[j] = ((Double)Math.floor( (points[i][j] - (blk_index.get(j)*edgeLg - overlapLg)) / cell_size)).intValue();
				nest_index[j] = ((Double)Math.floor( (points[i][j] - (blkStartPosPerDim.get(j) - overlapLg)) / cell_size)).intValue();
				if(nest_index[j] <0)
					nest_index[j] = 0;
			}
			for(int j = 0; j < num_dim; j ++){
				int multiple = 1;
				for(int k = 0; k < j; k++){
					multiple *= numCellsPerDim.get(k);
				}
				//				flat_index += nest_index[j] * Math.pow(num_cell_dim, j);
				flat_index += nest_index[j] * multiple;
			}
			// add this point into the corresponding cell
			this.cells.get(flat_index).addTuple(num_dim, points[i], pointIds[i]);
			//			this.cells.get(67).addTuple(num_dim, points[i]);
		}
	}

	/**
	 * Label qualified cells red
	 * @param m maximum number of objects within the D-neighborhood of an outlier, M =N(1-p)
	 */
	public void labelRed(int m) {
		for(int i = 0; i < cells.size(); i++){
			if(cells.get(i).count > m){
				cells.get(i).color = 2;		//2 -> red
			}
		}
	}

	/**
	 * Label red cells' layer1 cells pink
	 */
	public void labelRedCellLayer1Pink() {
		for(int i = 0; i < cells.size(); i++){
			if(cells.get(i).color == 2){
				// find Layer 1
				List<Integer> layer1 = findLayerCellsIndex(1,i);
				// label pink
				for(int j = 0; j < layer1.size(); j++){
					cells.get(layer1.get(j)).color = 1;
				}
			}
		}
	}

	/**
	 * find Layer 1 or Layer 2
	 * @param layerNum				indicate Layer 1 or 2; no thrid number
	 * @param red_cell_flat_index	centered red cell's flat index
	 * @return list of cells belonging to layer 1 or 2
	 */
	private List<Integer> findLayerCellsIndex(int layerNum, int red_cell_flat_index){
		List<Integer> index = null;		// list of cells which will return
		List<Integer> tmp_index;		// temporary list of cells for computation convenience

		// Layer 1
		if(layerNum == 1){
			index = findCellsByThickness(red_cell_flat_index, 1);
			tmp_index = findCellsByThickness(red_cell_flat_index, 0);
			for(int i = 0; i < tmp_index.size(); i++){
				index.remove(tmp_index.get(i));
			}
		}
		// Layer 2
		else if(layerNum == 2){
			int thickness = ((Double)Math.ceil(Math.sqrt(num_dim)*2)).intValue();
			index = findCellsByThickness(red_cell_flat_index, thickness);
			tmp_index = findCellsByThickness(red_cell_flat_index, 1);
			for(int i = 0; i < tmp_index.size(); i++){
				index.remove(tmp_index.get(i));
			}
		}
		else {
			//ERROR! there is no third layer in this algorithm
		}
		return index;
	}

	/**
	 * find cells by defined thickness; the number of cells should be at most (thickness*2+1)^(num_dim)
	 * @param red_cell_flat_index	centered cell's index
	 * @param thickness				number indicating thickness
	 * @return list of indexes defined by thickness
	 */
	private List<Integer> findCellsByThickness(int red_cell_flat_index, int thickness) {

		// flat index -> nest index
		int tmp = red_cell_flat_index;
		int nest_index[] = new int[num_dim];
		for(int i = 0; i < num_dim; i++){
			int multiple = 1;
			for(int k = 0; k < num_dim-i-1; k++){
				multiple *= numCellsPerDim.get(k);
			}
			//			nest_index[num_dim - i - 1] = red_cell_flat_index / ((Double)Math.pow(num_cell_dim, num_dim - i - 1)).intValue();
			nest_index[num_dim - i - 1] = red_cell_flat_index / multiple;
			//			red_cell_flat_index -= nest_index[num_dim - i - 1] * Math.pow(num_cell_dim, num_dim - i - 1);
			red_cell_flat_index -= nest_index[num_dim - i - 1] * multiple;
		}
		red_cell_flat_index = tmp;		//restore red_cell_flat_index

		// variables for calculating the specified cells' flat index due to the known thickness
		List<Integer> flat_index = new ArrayList<Integer>();			// final list of indexes
		List<Integer> tmp_flat_index = new ArrayList<Integer>();		// temporary list of indexes for computation necessary
		int cnt = 0;													// count for number of dimensions

		// For-loop for dimensions
		for(cnt = 0; cnt < num_dim; cnt ++){

			// Copy current indexes into temporary indexes for future multiplication
			tmp_flat_index.clear();
			for(int i2 = 0; i2 < flat_index.size(); i2++){
				tmp_flat_index.add(flat_index.get(i2));
			}
			flat_index.clear();

			// Calculate cells' flat indexes; the total number of these indexes should be at most (thickness*2+1)^(num_dim)
			for(int i = 0; i < thickness*2+1; i ++){
				int tmp_nest_index = nest_index[cnt] + i - thickness;	// tmp_nest_index is a variation of the nest_index[cnt]; 5,6,7 for 6 when thickness is 1
				// consider margin problem for losing nearby cells
				if(tmp_nest_index >=0 && tmp_nest_index < numCellsPerDim.get(cnt) ){
					// initialization for the first time when count is 0
					if(cnt == 0){
						flat_index.add(tmp_nest_index * 1 );
					}
					else {
						// calculate multiple
						int multiple = 1;
						for(int k = 0; k < cnt; k++){
							multiple *= numCellsPerDim.get(k);
						}
						// multiplication
						for(int j = 0; j < tmp_flat_index.size(); j++){
							flat_index.add(tmp_flat_index.get(j) + tmp_nest_index * multiple );
						}
					}
				}
			}
		}
		return flat_index;
	}

	/**
	 * Label non-empty white cells pink color
	 * @param m
	 * @param r
	 * @return
	 */
	public List<Tuple> labelNonEmptyWhiteCellsPink(int m, double r) {
		List<Tuple>outlier_tuples = new ArrayList<Tuple>();		//list to record outliers
		for(int i = 0; i < cells.size(); i++){
			if(cells.get(i).color == 0 && cells.get(i).count != 0){

				//				System.out.println("color=0&&count!=0: "+i);

				int cntw2 = cells.get(i).count;
				// find Layer 1
				List<Integer> layer1 = findLayerCellsIndex(1,i);
				for(int j = 0; j < layer1.size(); j++){
					cntw2 += cells.get(layer1.get(j)).count;
				}
				if(cntw2 > m){

					//					System.out.println("cntw2 > m; cntw2: "+cntw2+", m: "+m+", i: "+i);

					cells.get(i).color = 1;
				}
				else {
					int cntw3 = cntw2;
					// find Layer 2
					List<Integer> layer2 = findLayerCellsIndex(2,i);
					for(int j = 0; j < layer2.size(); j++){
						cntw3 += cells.get(layer2.get(j)).count;
					}
					if(cntw3 <= m){
						//						System.out.println("cntw3 <= m; cntw3: "+cntw3+", m: "+m+", i: "+i);  // none goes here

						for(int j = 0; j < cells.get(i).points.size(); j++){
							outlier_tuples.add(cells.get(i).points.get(j));
						}
					}
					else{
						//						System.out.println("cntw3 > m; cntw3: "+cntw3+", m: "+m+", i: "+i);

						for(int p = 0; p < cells.get(i).points.size(); p++){
							int cntp = cntw2;
							for(int j = 0; j < layer2.size(); j++){
								for(int q = 0; q < cells.get(layer2.get(j)).points.size(); q++){
									double dist = Tuple.distBetweenTwoTuples(cells.get(i).points.get(p), cells.get(layer2.get(j)).points.get(q));

									//									System.out.println("i: "+i+", p: "+p+"; j: "+j+", q: "+q+"; dist: "+dist);

									if(dist <= r){
										cntp ++;
									}
								}
							}

							if(cntp <= m){
								outlier_tuples.add(cells.get(i).points.get(p));
							}

						}
					}
				}
			}
		}
		return outlier_tuples;
	}

	/**
	 * print each cell and its related info in that cell in a cellStructure
	 * @param m 	minimum number of count in a cell
	 * @param clr	minimum number of color index
	 */
	public void print(int m, int clr) {
		int totalCount = 0;
		System.out.println("Cell Structure");
		System.out.println("Number of dimension: " + num_dim);
		//		System.out.println("Number of cells in one dimension: " + num_cell_dim);
		System.out.println("Size of one cell: " + cell_size);
		for(int i = 0; i < cells.size(); i++){
			if(cells.get(i).count >= m && cells.get(i).color >= clr){
				System.out.println("Cell " + i + ":");
				cells.get(i).print();
				totalCount += cells.get(i).count;
			}
		}
		System.out.println("Total count: " + totalCount);
		System.out.println("End print");
	}

}


