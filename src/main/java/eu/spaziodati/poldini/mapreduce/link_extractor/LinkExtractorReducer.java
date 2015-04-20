package eu.spaziodati.poldini.mapreduce.link_extractor;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import eu.spaziodati.poldini.avro.SimpleLink;

public class LinkExtractorReducer extends
		Reducer<Text, Text, org.apache.avro.mapred.AvroKey<SimpleLink>, org.apache.hadoop.io.NullWritable> {
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// valid variable became true only if there is a url coming from the
		// input dataset
		boolean valid = false;
		//Array to store all valid values used to reiterate the second time 
		ArrayList<String> valueList=new ArrayList<String>();
		for (Text value : values) {
			// if the value is empty it means that there is a url inside the
			// values list that belongs to the input
			// page dataset and the key is a valid url
			if (value.toString().equals("")) {
//				System.out.println("valid page: " + key);
				valid = true;
			}else{
				valueList.add(value.toString());
			}
		}
		
		if (valid) {
			for (String value : valueList) {
//				System.out.println(value + " , " +key );
				if (!value.equals("")) {
					context.write(new AvroKey<SimpleLink>(new SimpleLink(value, key.toString())), NullWritable.get());
				}
			}
		}

	}
}
