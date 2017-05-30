import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GrepLinux3 {

	private static ExecutorService executor;
	private static ExecutorService chunkingExecutor;
	private static final List<String> result_matchedLines = Collections.synchronizedList(new ArrayList<String>());

	static final Map<Integer,Set<String>> tempMap = new ConcurrentHashMap<>();

	/*
	 * This variable only holds the big file in chunks 
	 * integer holds the chunk number (ex: 0till 10 if you divide the file into 10)
	 * International_Atomic_Time> Category:Time_scales>
	 * Map<String,String> first string is the second split([1]) of the file because we compare it with category file
	 * Our purpose is to find entities who has path to our categories
	 *second str in the map whole corresponding line of the file
	 *  
	 */
	private static final Map<Integer, Map<String,List<String>>> bigFileChunks = new ConcurrentHashMap<>();


	public static void main(String[] args) {
		if (args.length != 3) 
		{
			System.err.println("wrong number of parameters");
			return;
		}


		final String str_smallFile = args[0];
		//final String str_bigFile = "/home/rtue/workspace/CategoryTreeGradle/skos_broader_CatCleaned_sort.txt";
		final String str_bigFile = args[1];
		final int numberOfCPU = Integer.parseInt(args[2]);

		LineNumberReader lnr=null;
		try 
		{
			lnr = new LineNumberReader(new FileReader(new File(str_bigFile)));
			lnr.skip(Long.MAX_VALUE);
		} 
		catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println("Number of CPU : "+numberOfCPU);
		System.out.println("Big File Line Number: "+lnr.getLineNumber());

		final int branchFactor = (lnr.getLineNumber())/ numberOfCPU;

		System.out.println("Branch Factor: "+branchFactor);


		executor = Executors.newFixedThreadPool(numberOfCPU+1);
		chunkingExecutor = Executors.newFixedThreadPool(numberOfCPU+1);
		System.out.println("Start reading file to set");
		readBigFileToSet(str_bigFile,branchFactor);
		System.out.println("End reading file to set");
		System.out.println(tempMap.size());
		System.out.println(tempMap.get(0).size());
	
		System.out.println("Start chunking");
		readChunkFromBigFileParallel(numberOfCPU);
		System.out.println("End chunking");
		
		System.err.println(bigFileChunks.size());
		
		
		try {
			for (int i = 0; i < numberOfCPU; i++) {
				final int startIndex = i;
				final Runnable task = () -> {
					System.out.println("Task " + startIndex + " started.");
					Map<String, List<String>> map = bigFileChunks.get(startIndex);
					if(map == null){
						System.err.println("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");
					}
					findMatchingLines(str_smallFile, map);
					System.out.println("Task " + startIndex + " finsihed.");
				};
				executor.execute(task);
			}

			executor.shutdown();
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

			writeResult("result");

		} catch (final Exception exception) {
			System.err.println(exception);
		}

	}

	private static void readChunkFromBigFileParallel(int numberOfCpu) {
		try{
			for (int i = 0; i < numberOfCpu; i++) {
				final int cpuNumber = i;
				final Runnable task = () -> {

					final Map<String,List<String>> chunk = new ConcurrentHashMap<>();
					try {
						final Set<String> set = tempMap.get(cpuNumber);
						for(String s: set){
							s = s.replaceAll("Category:", "").replaceAll(">", "");
							final String category = s.toLowerCase().split(" ")[1];

							final List<String> listValue = chunk.get(category);
							if(listValue==null){
								chunk.put(category,Arrays.asList(s.toLowerCase()));
							}else{
								ArrayList<String> newList = new ArrayList<>(listValue);
								newList.add(s.toLowerCase());
								chunk.put(category,newList);
							}
						}
						bigFileChunks.put(cpuNumber, new ConcurrentHashMap<>(chunk));
						
					} catch (Exception exception) {
						exception.printStackTrace();
					}
					System.out.println("Chunking is done for "+cpuNumber);
				};
				chunkingExecutor.execute(task);
			}
			chunkingExecutor.shutdown();
			chunkingExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	static void readBigFileToSet(final String str_bigFile,final int branchFactor) {
		int index = 0;
		Set<String> set = new HashSet<>();
		try {
			final BufferedReader br_bigFile = new BufferedReader(new FileReader(str_bigFile));
			String b_line;
			while ((b_line = br_bigFile.readLine()) != null) 
			{	
				set.add(b_line);
				if(set.size()==branchFactor){
					tempMap.put(index++,new HashSet<>(set));
					set = new HashSet<>();
				}
			}
			if(!set.isEmpty()){
				tempMap.put(index,new HashSet<>(set));
			}
			
			br_bigFile.close();
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

	
	public static void findMatchingLines(String str_smallFile, Map<String, List<String>> map) {
		BufferedReader br_smallFile;
		try {
			br_smallFile = new BufferedReader(new FileReader(str_smallFile));

			String s_line = null;
			while ((s_line = br_smallFile.readLine()) != null) {
				String str_small = s_line.toLowerCase();
				final List<String> valueInMap = map.get(str_small);
				if(valueInMap!=null && !valueInMap.isEmpty()){
					for(String s:valueInMap){
						result_matchedLines.add(s);
					}
				}
			}
			br_smallFile.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void writeResult(String str_resultFile) throws IOException {
		File file_result = new File(str_resultFile);
		file_result.createNewFile();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file_result, false));

		for (int i = 0; i < result_matchedLines.size(); i++) {
			bufferedWriter.write(result_matchedLines.get(i));
			bufferedWriter.newLine();
		}
		bufferedWriter.close();
		System.out
		.println("Finished Writing to a file: " + file_result.getName() + " " + file_result.getAbsolutePath());
	}


}

