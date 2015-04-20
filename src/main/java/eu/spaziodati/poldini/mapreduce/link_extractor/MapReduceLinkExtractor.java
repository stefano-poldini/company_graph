package eu.spaziodati.poldini.mapreduce.link_extractor;

import java.io.File;
import java.io.IOException;

import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import eu.spaziodati.poldini.avro.Page;
import eu.spaziodati.poldini.avro.SimpleLink;
import eu.spaziodati.poldini.mapreduce.page_filter.MapReducePageFilter;
import eu.spaziodati.poldini.utils.Utils;



/**
 * @author stefano
 * This mapreduce job takes as input a list of Pages instances and creates 
 * as output a list of SingleLinks. 
 * Every output SingleLink contains as target the url of a pointed link inside 
 * a Page content and as source the url of that Page. Only targets existing in
 * the domain (the input Page list) are valid.
 */

public class MapReduceLinkExtractor extends Configured implements Tool {

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: MapReduceLinkExtractor <input path> <output path>");
			return -1;
		}

		String inputFile = args[0];
		String outputFile = args[1];
		System.out.println(inputFile + "  " + outputFile);

		Configuration conf = getConf();
//		AvroSerialization.addToConfiguration(conf);
//        AvroSerialization.setValueWriterSchema(conf, SimpleLink.getClassSchema());
//        AvroSerialization.setValueReaderSchema(conf, SimpleLink.getClassSchema());
        
		Job job = new Job(conf);
		job.setJarByClass(MapReduceLinkExtractor.class);
		job.setJobName("Link Extractor");

		// set file input format
		FileInputFormat.setInputPaths(job, new Path(inputFile));

		// delete directory if outputFile directory exists
		File file = new File(outputFile);
		Utils.deleteDir(file);
		FileOutputFormat.setOutputPath(job, new Path(outputFile));


		job.setInputFormatClass(AvroKeyInputFormat.class);
		job.setMapperClass(LinkExtractorMapper.class);
		AvroJob.setInputKeySchema(job, Page.getClassSchema());
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputFormatClass(AvroKeyOutputFormat.class);
		job.setReducerClass(LinkExtractorReducer.class);
		AvroJob.setOutputKeySchema(job, SimpleLink.getClassSchema());
		job.setOutputValueClass(NullWritable.class);

		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
