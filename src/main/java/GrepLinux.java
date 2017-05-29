import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class GrepLinux {

	private static ExecutorService algorithmExecutor;
	private static final List<String> mainList = Collections.synchronizedList(new ArrayList<String>());

//	static int sum = 0;

	private static Map<Integer, Map<String,String>> map = new HashMap<>();

	public static void main(String[] args) {

		if (args.length != 3) {
			System.err.println("wrong number of parameters");
			return;
		}
		final String str_smallFile = args[0];
		//final String str_bigFile = "/home/rtue/workspace/CategoryTreeGradle/articleCategory_EntitiyBasedFiltered";//args[1];
		final String str_bigFile = args[1];
		final int numberOfCPU = Integer.parseInt(args[2]);
		//final int branchFactor = 3181 / numberOfCPU;
		final int branchFactor = 21652565 / numberOfCPU;

		algorithmExecutor = Executors.newFixedThreadPool(numberOfCPU);

		System.out.println("Start chunking");
		readChunkFromBigFile_rima(branchFactor, str_bigFile,numberOfCPU);
		//readChunkFromBigFile(branchFactor, str_bigFile);
		System.out.println("Chunking done");


//		map.entrySet().stream().forEach(p -> {
//			sum+=p.getValue().size();
//			System.err.println(p.getValue().size());
//		});
//
//		System.err.println("SUM "+sum);
//		System.err.println("Map Size= "+map.size());
//		System.err.println(numberOfCPU);
		try {
			for (int i = 0; i < numberOfCPU; i++) {
				final int startIndex = i;
				final Runnable task = () -> {
					System.out.println("Task " + startIndex + " started.");
					writeMatchingLinesToFile(str_smallFile, map.get(startIndex));
					System.out.println("Task " + startIndex + " finsihed.");
				};
				algorithmExecutor.execute(task);
			}

			algorithmExecutor.shutdown();
			algorithmExecutor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

			writeResult("result");

		} catch (final Exception exception) {
			System.err.println(exception);
		}

	}

	private static void readChunkFromBigFile_rima(int branchFactor, String str_bigFile, int numberOfCpu) {

		final Map<String,String> chunk = new HashMap<>();
		int lineCounter = 0;
		int index = 0;
		int start = 1;
		int end = start + branchFactor;
		try {
			BufferedReader br_bigFile = new BufferedReader(new FileReader(str_bigFile));
			String b_line;
			while ((b_line = br_bigFile.readLine()) != null) 
			{
				lineCounter++;
				b_line = b_line.replaceAll("Category:", "").replaceAll(">", "");
				if(index == numberOfCpu-1){
					chunk.put(b_line.toLowerCase().split(" ")[1],b_line.toLowerCase());
				}
				else{
					if (lineCounter >= start && lineCounter < end) {
						chunk.put(b_line.toLowerCase().split(" ")[1],b_line.toLowerCase());
					} else {

						map.put(index++, new HashMap<>(chunk));
						chunk.clear();
						start = (index * branchFactor)+index;
						end = start + branchFactor;
					}
				}
			}
			if(!chunk.isEmpty()){
				map.put(index++, new HashMap<>(chunk));
			}
			br_bigFile.close();
		} catch (Exception exception) {
			exception.printStackTrace();
		}
	}
	
//	private static List<String> readChunkFromBigFile(int branchFactor, String str_bigFile) {
//		List<String> chunk = new ArrayList<>();
//		int lineCounter = 0;
//		int index = 0;
//		int start = 0;
//		int end = start + branchFactor;
//		try {
//			BufferedReader br_bigFile = new BufferedReader(new FileReader(str_bigFile));
//			String b_line;
//			while ((b_line = br_bigFile.readLine()) != null) {
//				lineCounter++;
//				if (lineCounter >= start && lineCounter < end) {
//					chunk.add(b_line.toLowerCase());
//				} else {
//					map.put(index++, chunk);
//					chunk.clear();
//					start = index * branchFactor+1;
//					end = start + branchFactor;
//				}
//			}
//			br_bigFile.close();
//		} catch (Exception exception) {
//			exception.printStackTrace();
//		}
//		return chunk;
//	}

	public static void writeMatchingLinesToFile(String str_smallFile, Map<String, String> map2) {
		BufferedReader br_smallFile;
		try {
			br_smallFile = new BufferedReader(new FileReader(str_smallFile));

			String s_line = null;
			while ((s_line = br_smallFile.readLine()) != null) {
				String str_small = s_line.toLowerCase();
				final String valueInMap = map2.get(str_small);
				if(valueInMap!=null){
					mainList.add(valueInMap);
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

		for (int i = 0; i < mainList.size(); i++) {
			bufferedWriter.write(mainList.get(i));
			bufferedWriter.newLine();
		}
		bufferedWriter.close();
		System.out
		.println("Finished Writing to a file: " + file_result.getName() + " " + file_result.getAbsolutePath());
	}
}
