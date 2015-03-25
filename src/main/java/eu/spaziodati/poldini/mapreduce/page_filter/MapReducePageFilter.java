package eu.spaziodati.poldini.mapreduce.page_filter;

import java.io.File;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.util.Utils;

public class MapReducePageFilter extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[args.length - 1];
		System.out.println(inputFile + "  " + outputFile);

		Configuration conf = getConf();
		AvroSerialization.addToConfiguration(conf);
		// AvroSerialization.setKeyWriterSchema(conf, Page.getClassSchema());
		AvroSerialization.setValueWriterSchema(conf, Page.getClassSchema());
		// AvroSerialization.setKeyReaderSchema(conf,Page.getClassSchema());
		AvroSerialization.setValueReaderSchema(conf, Page.getClassSchema());

		Job job = new Job(conf);
		job.setJarByClass(MapReducePageFilter.class);
		job.setJobName("Page Filter");

		// set file input format
		for (int i = 0; i < args.length - 1; i++) {
			FileInputFormat.setInputPaths(job, new Path(args[i]));
		}

		// delete directory if outputFile directory exists
		File file = new File(outputFile);
		Utils.deleteDir(file);
		FileOutputFormat.setOutputPath(job, new Path(outputFile));

		// getConf().set("io.serializations","org.apache.avro.mapred.AvroSerialization<Page>");
		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(PageFilterMapper.class);
		AvroJob.setInputKeySchema(job, Page.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		// job.setMapOutputValueClass(Page.class);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(PageFilterReducer.class);
		AvroJob.setOutputKeySchema(job, Page.getClassSchema());
		AvroJob.setOutputValueSchema(job, Page.getClassSchema());

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
