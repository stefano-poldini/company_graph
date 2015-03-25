package eu.spaziodati.poldini.mapreduce.graph.creator;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.spaziodati.poldini.avro.SimpleLink;

public class GraphCreatorMapper extends Mapper<AvroKey<SimpleLink>, NullWritable, Text, Text> {
	@Override
	protected void map(AvroKey<SimpleLink> key, NullWritable value,
			Mapper<AvroKey<SimpleLink>, NullWritable, Text, Text>.Context context) throws IOException,
			InterruptedException {
		SimpleLink link = key.datum();
		context.write(new Text(link.getSource().toString()), new Text(link.getTarget().toString()));
	}
}