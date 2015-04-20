package eu.spaziodati.poldini.mapreduce.graph.creator;

import java.io.File;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import eu.spaziodati.poldini.avro.Link;
import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.avro.SimpleLink;
import eu.spaziodati.poldini.mapreduce.page_filter.MapReducePageFilter;
import eu.spaziodati.poldini.mapreduce.page_filter.PageFilterReducer;
import eu.spaziodati.poldini.utils.Utils;

public class MapReduceGraphCreator extends Configured implements Tool{

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MapReducePageCount <input path> <output path>");
			return -1;
		}
		// the input file has to be the output of MapReduceLinkExtractor
		String inputFile = args[0];
		String outputFile = args[1];
		System.out.println(inputFile + "  " + outputFile);

		Configuration conf = getConf();
        
		Job job = new Job(conf);
		job.setJarByClass(MapReduceGraphCreator.class);
		job.setJobName("Graph Creator");

		// set file input format
		FileInputFormat.setInputPaths(job, new Path(inputFile));

		// delete directory if outputFile directory exists
		File file = new File(outputFile);
		Utils.deleteDir(file);
		FileOutputFormat.setOutputPath(job, new Path(outputFile));


		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(GraphCreatorMapper.class);
		AvroJob.setInputKeySchema(job, SimpleLink.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(GraphCreatorReducer.class);
		AvroJob.setOutputKeySchema(job, Link.getClassSchema());
		job.setOutputValueClass(NullWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}
}
