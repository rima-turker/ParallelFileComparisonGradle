

import java.io.File;

public class GlobalVariables 
{
	public static final int levelOfTheTree=7;
	public static final String path_SkosFile= System.getProperty("user.dir")+ File.separator+ "skos_broader_CatCleaned_sort.txt";
	public static final String path_MainCategories= System.getProperty("user.dir")+File.separator+"MainCategoryFile.txt";
	public static final String path_Local = System.getProperty("user.dir") + File.separator;
	public static final String str_depthSeparator = "__";
	final static String str_testFileName = "TestSetDistinctPaths.csv";
	final static String str_top3FileName = "top_3Elements_Heu6_0.05.csv";
	final static String str_top1FileName = "top_1Elements_Heu6_0.05.csv";
	public static final String str_fileNameSkos=  "skos_broader_CatCleaned_sort.txt";
	
	//public static final String str_testFileName = "test_TestSetDistinctPaths.csv";
	public enum  HeuristicType 
	{
		//HEURISTIC_NO,
		HEURISTIC_NUMBEROFPATHS, 
		HEURISTIC_NUMBEROFPATHSANDDEPTH,
		HEURISTIC_FIRSTFINDFIRSTDEPTH,
		HEURISTIC_FIRSTFINDFIRSTDEPTHEXPONENTIAL,
		HEURISTIC_TFIDF,
		HEURISTIC_COMBINATION4TH5TH;
	}
}