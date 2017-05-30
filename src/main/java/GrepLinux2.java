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

public class GrepLinux2 {

	private static ExecutorService executor;
	private static final List<String> result_matchedLines = Collections.synchronizedList(new ArrayList<String>());
	private static final Set<String> bigFileSet = new HashSet<>();

	/*
	 * This variable only holds the big file in chunks 
	 * integer holds the chunk number (ex: 0till 10 if you divide the file into 10)
	 * International_Atomic_Time> Category:Time_scales>
	 * Map<String,String> first string is the second split([1]) of the file because we compare it with category file
	 * Our purpose is to find entities who has path to our categories
	 *second str in the map whole corresponding line of the file
	 *  
	 */
	private static Map<Integer, Map<String,List<String>>> hmap_bigFile = new ConcurrentHashMap<>();

	
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
		
		final int branchFactor = (lnr.getLineNumber() + 1)/ numberOfCPU;
		
		System.out.println("Branch Factor: "+branchFactor);

		executor = Executors.newFixedThreadPool(numberOfCPU);
		
		System.out.println("Start reading");
		long now = System.currentTimeMillis();
		readBigFileToSet(str_bigFile);
		System.out.println("Finish reading. "+TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()-now));
		
		
		
//		System.out.println("Start chunking");
//		readChunkFromBigFile(branchFactor, str_bigFile,numberOfCPU);
//		System.out.println("Chunking done");


//		map.entrySet().stream().forEach(p -> {
//			sum+=p.getValue().size();
//			System.err.println(p.getValue().size());
//		});

//		System.err.println("SUM "+sum);
//		System.err.println(numberOfCPU);
		try {
			for (int i = 0; i < numberOfCPU; i++) {
				final int startIndex = i;
				final Runnable task = () -> {
					System.out.println("Task " + startIndex + " started.");
					findMatchingLines(str_smallFile);
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

	private static void readBigFileToSet(String str_bigFile) {
		try {
			final BufferedReader br_bigFile = new BufferedReader(new FileReader(str_bigFile));
			String b_line;
			while ((b_line = br_bigFile.readLine()) != null) 
			{
				bigFileSet.add(b_line);
			}
			br_bigFile.close();
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}

//	private static void readChunkFromBigFile(int branchFactor, String str_bigFile, int numberOfCpu) {
//
//		final Map<String,List<String>> chunk = new ConcurrentHashMap<>();
//		int lineCounter = 0;
//		int index = 0;
//		int start = 1;
//		int end = start + branchFactor;
//		try {
//			BufferedReader br_bigFile = new BufferedReader(new FileReader(str_bigFile));
//			String b_line;
//			while ((b_line = br_bigFile.readLine()) != null) 
//			{
//				lineCounter++;
//				b_line = b_line.replaceAll("Category:", "").replaceAll(">", "");
//				String category = b_line.toLowerCase().split(" ")[1];
//				if(index == numberOfCpu-1){
//					final List<String> listValue = chunk.get(category);
//					if(listValue==null){
//						chunk.put(category,Arrays.asList(b_line.toLowerCase()));
//					}else{
//						ArrayList<String> newList = new ArrayList<>(listValue);
//						newList.add(b_line.toLowerCase());
//						chunk.put(category,newList);
//					}
//				}
//				else{
//					if (lineCounter >= start && lineCounter < end) {
//						final List<String> listValue = chunk.get(category);
//						if(listValue==null || listValue.isEmpty()){
//							chunk.put(category,Arrays.asList(b_line.toLowerCase()));
//						}else{
//							ArrayList<String> newList = new ArrayList<>(listValue);
//							newList.add(b_line.toLowerCase());
//							chunk.put(category,newList);
//						}												
//					} else {
//						hmap_bigFile.put(index++, new ConcurrentHashMap<>(chunk));
//						chunk.clear();
//						start = (index * branchFactor)+index;
//						end = start + branchFactor;
//					}
//				}
//			}
//			br_bigFile.close();
//			if(!chunk.isEmpty()){
//				hmap_bigFile.put(index++, new ConcurrentHashMap<>(chunk));
//			}
//		} catch (Exception exception) {
//			exception.printStackTrace();
//		}
//	}
	

	public static void findMatchingLines(String str_smallFile) {
		BufferedReader br_smallFile;
		int count =1;
		try {
			br_smallFile = new BufferedReader(new FileReader(str_smallFile));

			String s_line = null;
			while ((s_line = br_smallFile.readLine()) != null) {
				String str_small = s_line.toLowerCase();
				for(String str_bline : bigFileSet)
				{
					if(str_bline.contains(str_small)){
						result_matchedLines.add(str_bline);
					}
				}
				System.err.println(count++);
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

