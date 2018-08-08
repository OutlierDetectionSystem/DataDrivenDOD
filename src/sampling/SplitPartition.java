package sampling;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import util.SQConfig;

/**
 * 
 * derive the summary from all reducer for each partition
 * 
 * @author Yizhou Yan, modified luwei
 *
 */
public class SplitPartition {

	private static float[][] partition_store;
	private static int num_dims = 2;
	private static float[][] new_partition_store;
	private static int di_numBuckets;

	public SplitPartition(int di_numBuckets, int num_dims) {
		this.num_dims = num_dims;
		this.di_numBuckets = di_numBuckets;
		partition_store = new float[(int) Math.pow(di_numBuckets, num_dims)][num_dims * 2];
		new_partition_store = new float[(int) Math.pow(di_numBuckets, num_dims) * 2][num_dims * 4];
	}

	public void mergeAll(String input_dir, String output) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] stats = fs.listStatus(new Path(input_dir));

		for (int i = 0; i < stats.length; ++i) {
			if (!stats[i].isDirectory()) {
				/** parse file */
				parseFile(fs, stats[i].getPath().toString());
			}
		}
		int newPNum = dealWithLargePartitions();
		writeToHDFS(fs, output, newPNum);
		fs.close();
	}

	public int dealWithLargePartitions() {
		int maxLimit = 2500;
		int totalNumberOfPartition = (int) Math.pow(di_numBuckets, num_dims);
		int countNew = 0;
		for (int i = 0; i < totalNumberOfPartition; i++) {
			System.out.println(
					"Input: " + partition_store[i][0] + "," + partition_store[i][1] + "," + partition_store[i][2] + ","
							+ partition_store[i][3] + "," + partition_store[i][4] + "," + partition_store[i][5]);
			if (partition_store[i][1] > 0 && partition_store[i][3] > 0 && partition_store[i][5] > 0) {
				for (float m = partition_store[i][0]; m < partition_store[i][1]; m = m + maxLimit)
					for (float n = partition_store[i][2]; n < partition_store[i][3]; n = n + maxLimit)
						for (float j = partition_store[i][4]; j < partition_store[i][5]; j = j + maxLimit) {
							new_partition_store[countNew][0] = m;
							new_partition_store[countNew][1] = Math.min(m + maxLimit, partition_store[i][1]);
							new_partition_store[countNew][2] = n;
							new_partition_store[countNew][3] = Math.min(n + maxLimit, partition_store[i][3]);
							new_partition_store[countNew][4] = j;
							new_partition_store[countNew][5] = Math.min(j + maxLimit, partition_store[i][5]);
							System.out.println("New: " + new_partition_store[countNew][0] + ","
									+ new_partition_store[countNew][1] + "," + new_partition_store[countNew][2] + ","
									+ new_partition_store[countNew][3] + "," + new_partition_store[countNew][4] + ","
									+ new_partition_store[countNew][5]);
							countNew++;
						}
			}
		}
		return countNew;
	}

	/**
	 * format of each line: pid, min_r, max_r, size, number, kNN dist list
	 * 
	 * @param fs
	 * @param filename
	 */
	public void parseFile(FileSystem fs, String filename) {
		try {
			// check if the file exists
			Path path = new Path(filename);
			// System.out.println("filename = " + filename);
			if (fs.exists(path)) {
				FSDataInputStream currentStream;
				BufferedReader currentReader;
				currentStream = fs.open(path);
				currentReader = new BufferedReader(new InputStreamReader(currentStream));
				String line;
				while ((line = currentReader.readLine()) != null) {
					/** parse line */
					String[] values = line.split(SQConfig.sepStrForRecord);
					int ppid = Integer.valueOf(values[0]);
					for (int i = 1; i < num_dims * 2 + 1; i++) {
						partition_store[ppid][i - 1] = Float.valueOf(values[i]);
					}
				}
				currentReader.close();
			} else {
				throw new Exception("the file is not found .");
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	private void writeToHDFS(FileSystem fs, String output_dir, int newPNum) {
		double sumOfDiameter = 0;
		try {
			String filename = output_dir + "/pp-r-00000";
			Path path = new Path(filename);
			System.out.println("output path:" + path);
			FSDataOutputStream currentStream;
			currentStream = fs.create(path, true);
			String line;
			/** writhe the summary information */
			for (int i = 0; i < newPNum; i++) {
				line = i + "," + new_partition_store[i][0] + ","
						+ new_partition_store[i][1] + "," + new_partition_store[i][2] + ","
						+ new_partition_store[i][3] + "," + new_partition_store[i][4] + ","
						+ new_partition_store[i][5];
				
				line += "\n";
				currentStream.writeBytes(line);
			}
			currentStream.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/core-site.xml"));
		conf.addResource(new Path("/usr/local/Cellar/hadoop/etc/hadoop/hdfs-site.xml"));
		new GenericOptionsParser(conf, args).getRemainingArgs();
		try {
			// String input = args[0];
			String input = conf.get(SQConfig.strPartitionPlanOutput);
			String output = conf.get(SQConfig.strRefinedPPOutput);

			int di_numBuckets = conf.getInt(SQConfig.strNumOfPartitions, 2);
			int num_dims = conf.getInt(SQConfig.strDimExpression, 2);
			SplitPartition ms = new SplitPartition(di_numBuckets, num_dims);
			long begin = System.currentTimeMillis();
			ms.mergeAll(input, output);
			long end = System.currentTimeMillis();
			long second = (end - begin) / 1000;
			System.err.println("MergeSummary takes " + second + " seconds");
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
