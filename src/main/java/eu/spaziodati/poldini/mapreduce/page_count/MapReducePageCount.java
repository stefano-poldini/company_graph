package eu.spaziodati.poldini.mapreduce.page_count;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.utils.Utils;

public class MapReducePageCount extends Configured implements Tool {

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("Usage: MapReducePageCount <input path> <output path>");
			return -1;
		}

		System.out.println(args[0] + "  " + args[1]);
		Job job = new Job(getConf());
		job.setJarByClass(MapReducePageCount.class);
		job.setJobName("Page Count");

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		File file = new File(args[1]);
		Utils.deleteDir(file);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(PageCountMapper.class);
		AvroJob.setInputKeySchema(job, Page.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
		job.setReducerClass(PageCountReducer.class);
		AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
		AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.INT));

		return (job.waitForCompletion(true) ? 0 : 1);
	}

	

}
