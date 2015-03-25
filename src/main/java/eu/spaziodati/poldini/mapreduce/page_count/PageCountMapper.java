package eu.spaziodati.poldini.mapreduce.page_count;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.spaziodati.poldini.avro.Page;

public class PageCountMapper extends Mapper<AvroKey<Page>, NullWritable, Text, IntWritable> {
	@Override
    public void map(AvroKey<Page> key, NullWritable value, Context context)
        throws IOException, InterruptedException {

      CharSequence url = key.datum().getUrl();
      if (url != null && !url.equals("")) {
          context.write(new Text(""), new IntWritable(1));
      }
    }
}
