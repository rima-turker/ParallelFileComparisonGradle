import static org.junit.Assert.*;

import org.junit.Test;

public class GrepLinux3Test {

	@Test
	public void test() {
		String testFile ="/home/rtue/GitHubProjects/ParallelFileComparisonGradle/src/test/resource/test.txt";
		int branchFactor = 1;
		GrepLinux3.readBigFileToSet(testFile, branchFactor);
		assertEquals(10, GrepLinux3.tempMap.size());
		for(int i=0;i<10;i++){
			assertEquals(branchFactor, GrepLinux3.tempMap.get(i).size());
		}
		
		branchFactor = 10;
		GrepLinux3.tempMap.clear();
		GrepLinux3.readBigFileToSet(testFile, branchFactor);
		assertEquals(1, GrepLinux3.tempMap.size());
		assertEquals(branchFactor, GrepLinux3.tempMap.get(0).size());
		
		branchFactor = 2;
		GrepLinux3.tempMap.clear();
		GrepLinux3.readBigFileToSet(testFile, branchFactor);
		assertEquals(5, GrepLinux3.tempMap.size());
		for(int i=0;i<5;i++){
			assertEquals(branchFactor, GrepLinux3.tempMap.get(i).size());
		}
		
		branchFactor = 3;
		GrepLinux3.tempMap.clear();
		GrepLinux3.readBigFileToSet(testFile, branchFactor);
		assertEquals(4, GrepLinux3.tempMap.size());
		for(int i=0;i<3;i++){
			assertEquals(branchFactor, GrepLinux3.tempMap.get(i).size());
		}
		assertEquals(10 - branchFactor*3, GrepLinux3.tempMap.get(3).size());
	}

}
