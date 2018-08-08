package dod;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import metricspace.IMetric;
import metricspace.IMetricSpace;
import metricspace.MetricSpaceUtility;
import metricspace.Record;
import sampling.CellStore;
import util.SQConfig;

public class FindOutliers {
	/**
	 * default Map class.
	 *
	 * @author Yizhou Yan
	 * @version Dec 31, 2015
	 */

	public static class FindOutliersMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		/**
		 * number of small cells per dimension: when come with a node, map to a
		 * range (divide the domain into small_cell_num_per_dim) (set by user)
		 */
		public static int cell_num = 501;

		/** The domains. (set by user) */
		private static float[] domains;
		/** size of each small buckets */
		private static int smallRange;
		/**
		 * block list, which saves each block's info including start & end
		 * positions on each dimension. print for speed up "mapping"
		 */
		private static float[][] partition_store;
		/** save each small buckets. in order to speed up mapping process */
		private static CellStore[] cell_store;
		/**
		 * Number of desired partitions in each dimension (set by user), for
		 * Data Driven partition
		 */
		private static int di_numBuckets;

		private static int K;

		private static float radius;

		private static float overlapLg;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			cell_num = conf.getInt(SQConfig.strNumOfSmallCells, 501);
			domains = new float[2];
			domains[0] = conf.getFloat(SQConfig.strDomainMin, 0.0f);
			domains[1] = conf.getFloat(SQConfig.strDomainMax, 10001.0f);
			smallRange = (int) Math.ceil((domains[1] - domains[0]) / cell_num);
			cell_store = new CellStore[(int) Math.pow(cell_num, num_dims)];
			di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);
			partition_store = new float[(int) Math.pow(di_numBuckets, num_dims)][num_dims * 2];
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			radius = conf.getFloat(SQConfig.strRadius, 1.0f);
			overlapLg = radius + 0.01f;
			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 2) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp")) {
							System.out.println("Reading partition plan from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */

								String[] splitsStr = line.split(SQConfig.sepStrForRecord);
								int tempid = Integer.parseInt(splitsStr[0]);
								for (int j = 1; j < num_dims * 2 + 1; j++) {
									partition_store[tempid][j - 1] = Float.parseFloat(splitsStr[j]);
									// System.out.print(partition_store[tempid][j
									// - 1] + ",");
								}
							}
							currentReader.close();
							currentStream.close();
						} else if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("part")) {
							System.out.println("Reading cells for partitions from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */
								String[] items = line.split(SQConfig.sepStrForRecord);
								if (items.length == 3) {
									int cellId = Integer.parseInt(items[0]);
									int corePartitionId = Integer.valueOf(items[1].substring(2));
									cell_store[cellId] = new CellStore(cellId, corePartitionId);
									if (items[2].length() > 1) { // has support
																	// cells
										String[] splitStr = items[2].substring(2, items[2].length())
												.split(SQConfig.sepSplitForIDDist);
										for (int j = 0; j < splitStr.length; j++) {
											cell_store[cellId].support_partition_id.add(Integer.valueOf(splitStr[j]));
										}
									}
									// System.out.println(cell_store[cellId].printCellStoreWithSupport());
								}
							}
							currentReader.close();
							currentStream.close();
						} // end else if
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}
			System.out.println("End Setting up");
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Variables
			// input format key:nid value: point value, partition id,
			// k-distance, (KNN's nid and dist),tag
			String[] splitStr = value.toString().split(",");
			float[] crds = new float[num_dims];
			// parse raw input data into coordinates/crds
			for (int i = 1; i < num_dims + 1; i++) {
				crds[i - 1] = Float.parseFloat(splitStr[i]);
			}
			int cell_id = CellStore.ComputeCellStoreId(crds, num_dims, cell_num, smallRange);
			if (cell_id < 0)
				return;

			int corePartitionId = cell_store[cell_id].core_partition_id;
			if (corePartitionId < 0)
				return;

			Set<Integer> partitionToCheck = new HashSet<Integer>();
			for (Iterator itr = cell_store[cell_id].support_partition_id.iterator(); itr.hasNext();) {
				int keyiter = (Integer) itr.next();
				partitionToCheck.add(keyiter);
			}

			for (Iterator iter = partitionToCheck.iterator(); iter.hasNext();) {
				int blk_id = (Integer) iter.next();
				if (blk_id < 0) {
					System.out.println("Block id: " + blk_id);
					continue;
				}
				// for(int blk_id=0;blk_id<markEndPoints(crds[0]);blk_id++) {
				int belong = 0; // indicate whether the point belongs, 0 ->
								// neither; 1 -> regular; 2-> extend
				// traverse block's start & end positions in each dimension
				for (int i = 0; i < num_dims; i++) {
					if (crds[i] < partition_store[blk_id][2 * i + 1] + overlapLg
							&& crds[i] >= partition_store[blk_id][2 * i] - overlapLg) {
						belong = 2;
					} else {
						belong = 0;
						break;
					}
				} // end for(int i=0;i<numDims;i++)

				// output block key and data value
				if (belong == 2) { // support area data
					// output to support area with a tag 'S'
					String str = value.toString();
					context.write(new IntWritable(blk_id), new Text(str + 'S'));
				} // end if
			} // end for(int blk_id=0;blk_id<blocklist.length;blk_id++)

			// output core area

			String str = value.toString();
			context.write(new IntWritable(corePartitionId), new Text(str + 'C'));

		}// end map function
	} // end map class

	public static class FindOutliersReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		/**
		 * The dimension of data (set by user, now only support dimension of 2,
		 * if change to 3 or more, has to change some codes)
		 */
		private static int num_dims = 2;
		private static int K;
		/** use cell-based algorithm, if true; nested-loop otherwise. */
		private static boolean useCellBasedAlgo = false;
		private IMetricSpace metricSpace = null;
		private IMetric metric = null;
		private static float[][] partition_store;
		private static float radius;
		private static int di_numBuckets;
		private static float overlapLg;
		
		/**
		 * get MetricSpace and metric from configuration
		 * 
		 * @param conf
		 * @throws IOException
		 */
		private void readMetricAndMetricSpace(Configuration conf) throws IOException {
			try {
				metricSpace = MetricSpaceUtility.getMetricSpace(conf.get(SQConfig.strMetricSpace));
				metric = MetricSpaceUtility.getMetric(conf.get(SQConfig.strMetric));
				metricSpace.setMetric(metric);
			} catch (InstantiationException e) {
				throw new IOException("InstantiationException");
			} catch (IllegalAccessException e) {
				e.printStackTrace();
				throw new IOException("IllegalAccessException");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
				throw new IOException("ClassNotFoundException");
			}
		}

		public void setup(Context context) throws IOException {
			Configuration conf = context.getConfiguration();
			readMetricAndMetricSpace(conf);
			/** get configuration from file */
			num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			K = Integer.valueOf(conf.get(SQConfig.strK, "1"));
			radius = conf.getFloat(SQConfig.strRadius, 1.0f);
			di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);
			partition_store = new float[(int) Math.pow(di_numBuckets, num_dims)][num_dims * 2];
			overlapLg = radius + 0.01f;
			/** parse files in the cache */
			try {
				URI[] cacheFiles = context.getCacheArchives();

				if (cacheFiles == null || cacheFiles.length < 2) {
					System.out.println("not enough cache files");
					return;
				}
				for (URI path : cacheFiles) {
					String filename = path.toString();
					FileSystem fs = FileSystem.get(conf);

					FileStatus[] stats = fs.listStatus(new Path(filename));
					for (int i = 0; i < stats.length; ++i) {
						if (!stats[i].isDirectory() && stats[i].getPath().toString().contains("pp")) {
							System.out.println("Reading partition plan from " + stats[i].getPath().toString());
							FSDataInputStream currentStream;
							BufferedReader currentReader;
							currentStream = fs.open(stats[i].getPath());
							currentReader = new BufferedReader(new InputStreamReader(currentStream));
							String line;
							while ((line = currentReader.readLine()) != null) {
								/** parse line */
								String[] splitsStr = line.split(SQConfig.sepStrForRecord);
								int tempid = Integer.parseInt(splitsStr[0]);
								for (int j = 1; j < num_dims * 2 + 1; j++) {
									partition_store[tempid][j - 1] = Float.parseFloat(splitsStr[j]);
									// System.out.print(partition_store[tempid][j
									// - 1] + ",");
								}
							}
							currentReader.close();
							currentStream.close();
						}
					} // end for (int i = 0; i < stats.length; ++i)
				} // end for (URI path : cacheFiles)

			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files");
			}

		}

		/**
		 * default Reduce class.
		 * 
		 * @author Yizhou Yan
		 * @version Dec 31, 2015
		 * @throws InterruptedException
		 */

		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<Record> core_data = new ArrayList<Record>();
			List<Record> support_data = new ArrayList<Record>();
			List<Record> all_data = new ArrayList<Record>();

			int numData = 0;
			Text val_outlier = new Text(""); // value for outliers

			for (Text value : values) {
				// parse values to get tmp_data
				char flg = value.toString().charAt(value.toString().length() - 1);
				String tempString = value.toString().substring(0, value.toString().length() - 2);
				Record obj = (Record) metricSpace.readObject(tempString, num_dims);

				// put tmp_data into correct array list depending on its flag
				if (flg == 'C') {
					core_data.add(obj);
				} else if (flg == 'S') {
					support_data.add(obj);
				} else {
					throw new RuntimeException(
							"No third options but regular or extend when putting data into list in reduce function");
				}
			} // end while (values.hasNext())
			all_data.addAll(core_data);
			all_data.addAll(support_data);
			numData = all_data.size();

			System.out.println("Points in this partition: " + numData);
			/*
			 * Nest-loop algorithm
			 */
			if (!useCellBasedAlgo) {
				// List<Record> outlier = new ArrayList<Record>();
				// find and store r-neighbors
				int tempCountNeighbors = 0;
				for (int i = 0; i < core_data.size(); i++) {
					Record data_i = core_data.get(i);
					// find and count r-neighbors from all data listed after
					// data i
					tempCountNeighbors = 0;
					for (int j = 0; j < all_data.size(); j++) {
						// earlier termination
						if (tempCountNeighbors >= K) {
							break;
						}
						if (core_data.get(i).equals(all_data.get(j)))
							continue;
						Record data_j = all_data.get(j);
						// compute distance
						float temp_dist = metric.dist(data_i, data_j);
						// record count of r-neighbor
						if (temp_dist <= radius)
							tempCountNeighbors += 1;
					}
					if (tempCountNeighbors < K) {
						// outlier.add(core_data.get(i));
						// output outlier id
						String valueString = core_data.get(i).getRId() + "";
						context.write(NullWritable.get(), new Text(valueString));
					}
				} // end for(int i = 0; i < rgl_data.size(); i ++)
			} // end if
			else {
				// size of a cell
				float cell_size = (float) (radius / (2 * Math.sqrt(num_dims)));
				// number of cells along each dimension
				List<Integer> numCellsPerDim = new ArrayList<Integer>();
				// list of starting position of this partition along each
				// dimension
				List<Float> blkStartPosPerDim = new ArrayList<Float>();
				// list of ending position of this partition along each
				// dimension
				List<Float> blkEndPosPerDim = new ArrayList<Float>();
				// double-list containing all data in this block & extended area
				float[][] points = new float[numData][num_dims];
				long [] pointIds = new long[numData];
				// list of outlier tuples
				List<Tuple> outlier_tuples = new ArrayList<Tuple>();
				// temporary list of outlier tuples, containing all in both
				// regular and extended areas
				List<Tuple> tmp_outlier_tuples = new ArrayList<Tuple>();
				// get list of start & end positions of this block on each
				// dimension
				for (int i = 0; i < num_dims; i++) {
					float tmp_start_pos = partition_store[key.get()][2 * i];
					float tmp_end_pos = partition_store[key.get()][2 * i + 1];
					blkStartPosPerDim.add(i, tmp_start_pos);
					blkEndPosPerDim.add(i, tmp_end_pos);
				}
				// get number of cells along each dimension
				for (int i = 0; i < num_dims; i++) {
					numCellsPerDim.add(((Double) Math
							.ceil((blkEndPosPerDim.get(i) - blkStartPosPerDim.get(i) + overlapLg * 2) / cell_size))
									.intValue());
				}

				// build points double-list containing all data in this block
				for (int i = 0; i < numData; i++) {
					points[i] = new float[num_dims];
					points[i] = all_data.get(i).getValue();
					pointIds[i] = all_data.get(i).getRId();
				}

				// Step 1 - Construct cells
				CellStruct cellStruct = new CellStruct(num_dims, numData, numCellsPerDim, cell_size, overlapLg,
						blkStartPosPerDim);

				// Step 2 - Map points into appropriate cells in a cell structure
				cellStruct.mapPointToCell(points, pointIds);

				// Step 3
				cellStruct.labelRed(K);

				// Step 4
				cellStruct.labelRedCellLayer1Pink();

				// Step 5
				tmp_outlier_tuples = cellStruct.labelNonEmptyWhiteCellsPink(K, radius);

				// only count outlier in regular block; in other words, kick out
				// the 'outlier' in extended area
				int counted = 1; // 1-> count; 0 -> not count
				for (int i = 0; i < tmp_outlier_tuples.size(); i++) {
					counted = 1;
					for (int j = 0; j < tmp_outlier_tuples.get(i).elements.size(); j++) {
						if (tmp_outlier_tuples.get(i).elements.get(j) < blkStartPosPerDim.get(j)
								|| tmp_outlier_tuples.get(i).elements.get(j) >= blkEndPosPerDim.get(j)) {
							counted = 0;
							break;
						}
					}
					if (counted == 1) {
						outlier_tuples.add(tmp_outlier_tuples.get(i));
					}
				}

				// output point id
				for (int i = 0; i < outlier_tuples.size(); i++) {
					String tmp = "";
					tmp += outlier_tuples.get(i).pointId;
					context.write(NullWritable.get(), new Text(tmp));
				}
//				// build values for outlier only for cell-based algorithm
//				String tmp = "";
//				for (int i = 0; i < outlier_tuples.size(); i++) {
//					tmp = "";
//					for (int j = 0; j < outlier_tuples.get(i).elements.size(); j++) {
//						tmp += ((Float) outlier_tuples.get(i).elements.get(j)).toString();
//						if (j != outlier_tuples.get(i).elements.size() - 1) {
//							tmp += ",";
//						}
//					}
//					context.write(NullWritable.get(), new Text(tmp));
//					
//				}
			}
		}
	}

	public void run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		/** set job parameter */
		Job job = Job.getInstance(conf, "DDLOF-calculate kdistance 2nd job");

		job.setJarByClass(FindOutliers.class);
		job.setMapperClass(FindOutliersMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setReducerClass(FindOutliersReducer.class);
		job.setNumReduceTasks(conf.getInt(SQConfig.strNumOfReducers, 1));
		// job.setNumReduceTasks(0);

		String strFSName = conf.get("fs.default.name");
		FileInputFormat.addInputPath(job, new Path(conf.get(SQConfig.dataset)));
		FileSystem fs = FileSystem.get(conf);
		fs.delete(new Path(conf.get(SQConfig.strFinalOutput)), true);
		FileOutputFormat.setOutputPath(job, new Path(conf.get(SQConfig.strFinalOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strPartitionPlanOutput)));
		job.addCacheArchive(new URI(strFSName + conf.get(SQConfig.strCellsOutput)));

		/** print job parameter */
		System.err.println("# of dim: " + conf.getInt(SQConfig.strDimExpression, 10));
		long begin = System.currentTimeMillis();
		job.waitForCompletion(true);
		long end = System.currentTimeMillis();
		long second = (end - begin) / 1000;
		System.err.println(job.getJobName() + " takes " + second + " seconds");
	}

	public static void main(String[] args) throws Exception {
		FindOutliers findKnnAndSupporting = new FindOutliers();
		findKnnAndSupporting.run(args);
	}
}
