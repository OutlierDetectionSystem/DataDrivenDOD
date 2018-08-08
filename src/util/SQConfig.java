package util;

public class SQConfig {
	/**============================ basic threshold ================ */
	/** metric space */
	public static final String strMetricSpace = "dod.metricspace.dataspace";  
	/** metric */
	public static final String strMetric = "dod.metricspace.metric";
	/** number of K */
	public static final String strK = "dod.threshold.K";
	/** number of radius*/
	public static final String strRadius = "dod.threshold.radius";
	/** number of dimensions */
	public static final String strDimExpression = "dod.vector.dim";
	/** dataset original path*/
	public static final String dataset = "dod.dataset.input.dir";   
	/** number of reducers */
	public static final String strNumOfReducers = "dod.reducer.count";
	
	/**============================= seperators ================ */
	/** seperator for items of every record in the index */
	public static final String sepStrForRecord = ",";
	public static final String sepStrForKeyValue = "\t";
	public static final String sepStrForIDDist = "|";
	public static final String sepSplitForIDDist = "\\|";
	
	/**============================= data driven sampling ================ */
	/** domain values */
	public static final String strDomainMin = "dod.sampling.domain.min";
	public static final String strDomainMax = "dod.sampling.domain.max";
	/** number of partitions */
	public static final String strNumOfPartitions = "dod.sampling.partition.count";
	/** number of small cells per dimension */
	public static final String strNumOfSmallCells = "dod.sampling.cells.count";
	/** sampling percentage 1% = 100 or 0.1% = 1000 */
	public static final String strSamplingPercentage = "dod.sampling.percentage";
	/** path for sampling*/
	public static final String strSamplingOutput = "dod.sampling.output";
	public static final String strPartitionPlanOutput = "dod.sampling.partitionplan.output";
	public static final String strRefinedPPOutput = "dod.sampling.partitionplan.refined";
	public static final String strCellsOutput = "dod.sampling.cells.output";
	
	/**============================= knn find first round =================== */
	public static final String strFinalOutput = "dod.outlier.output";
	public static final String strIndexFilePath = "dod.knnfind.cells.indexfile";
}
