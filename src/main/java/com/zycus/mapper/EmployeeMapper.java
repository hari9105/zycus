package com.zycus.mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class EmployeeMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static HashMap<String, String> employeeMap = new HashMap<>();
	private BufferedReader brReader;
	private String empDetails = "";
	private Text mapOutputKey = new Text("");
	private Text mapOutputValue = new Text("");

	enum MYCOUNTER {
		RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
	}
	

	//getting the file from cache and loading into hashmap for processing
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		for (Path eachPath : cacheFilesLocal) {
			// System.out.println(eachPath.getName()+" name
			// >>>>>>>>>>>>>>>>>>>>>>>>>>>>>. ");
			if (eachPath.getName().toString().trim().equals("File1.txt")) {
				context.getCounter(MYCOUNTER.FILE_EXISTS).increment(1);
				loadEmployeeHashMap(eachPath, context);
			}
		}
	}

	private void loadEmployeeHashMap(Path path, Context context) throws IOException {

		String lineRead = "";
		try {
			brReader = new BufferedReader(new FileReader(path.toString()));
			while ((lineRead = brReader.readLine()) != null) {
				String empField[] = lineRead.split("\\t");
				String empDetails = empField[3].trim() + "," + empField[4].trim(); //setting the salary and age in this field only
				employeeMap.put(empField[1].trim(), empDetails);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (brReader != null) {
				brReader.close();
			}
		}
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		context.getCounter(MYCOUNTER.RECORD_COUNT).increment(1);

		if (value.toString().length() > 0) {
			String empAttributes[] = value.toString().split("\\t");

			int salary = Integer.parseInt(empAttributes[1]);  //getting the salary from 2nd file
			int age = Integer.parseInt(empAttributes[4]);      //getting the age from 2nd file
			empDetails = employeeMap.get(empAttributes[3].toString());  //getting the empDetails from the cache if empName is same
			if (!(empDetails.equals(null) && empDetails.equals(""))) {
				String empDtls[] = empDetails.split(",");
				int empMapSalary = Integer.parseInt(empDtls[0]);
				int empMapage = Integer.parseInt(empDtls[1]);
				if ((salary > empMapSalary) && (age > empMapage)) {  //checking the condition for cumulative salary and higher age from both file
					int increasedSalary = salary - empMapSalary;
					mapOutputKey.set(empAttributes[3].toString());
					mapOutputValue.set(increasedSalary + "\t" + empAttributes[0].toString());
				}
			}
		}
		context.write(mapOutputKey, mapOutputValue);
		empDetails = "";
	}

}
