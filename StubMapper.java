package my;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StubMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		//A4-P1-3b

		String[] words = value.toString().split("[ \t]+");

		String wordRev=words[1];
		wordRev = new StringBuilder(wordRev).reverse().toString();
		if( words[1].compareTo(wordRev)>0 ){ 
			words[1] = wordRev; 
		}
		
		context.write(new Text(words[1]), new Text(words[0]));
		//System.out.println("MAP: " + words[1] + " " + words[0]);
	}
}
