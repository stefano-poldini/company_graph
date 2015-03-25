package eu.spaziodati.poldini.mapreduce.graph.creator;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import eu.spaziodati.poldini.avro.Link;

public class GraphCreatorReducer extends Reducer <Text, Text, AvroKey<Link>, NullWritable>{

	@Override
	protected void reduce(Text key, Iterable<Text> values,
			Reducer<Text, Text, AvroKey<Link>, NullWritable>.Context context) throws IOException,
			InterruptedException {
		Link link=new Link();
		link.setUrl(key.toString());
		for (Text target : values) {
			link.getLinks().add(target.toString());
		}
		context.write(new AvroKey<Link>(link), NullWritable.get());
	}
}
