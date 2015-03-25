package eu.spaziodati.poldini.mapreduce.page_count;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageCountReducer extends Reducer<Text, IntWritable, AvroKey<CharSequence>, AvroValue<Integer>> {

	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
			InterruptedException {

		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		context.write(new AvroKey<CharSequence>("Page Count"), new AvroValue<Integer>(sum));

	}
}
