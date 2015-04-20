package eu.spaziodati.poldini.mapreduce.page_filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import eu.spaziodati.poldini.avro.Page;

public class PageFilterReducer extends Reducer<Text, AvroValue<Page>, AvroKey<Page>, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<AvroValue<Page>> values, Context context) throws IOException,
			InterruptedException {
		boolean valid = false;
		
		//Array to store all valid values used to reiterate the second time 
		ArrayList<Page> pageList = new ArrayList<Page>();
		
		for (AvroValue<Page> value : values) {
			Page page = value.datum();
//			if (key.toString().contains("google")){
//				System.out.println(key + " , " + page);
//			}
			
			// if the content of the page is empty it means that there is a
			// datagem page inside the values list
			// and the key is a valid url
			if (page.getContent().equals("datagem") && page.getDebug().equals("datagem")) {
				valid = true;
			} else {
				pageList.add(Page.newBuilder(page).build());
			}
		}
		// if there is a Page coming from datagem in the values list we write
		// all crawler pages to the output file
		if (valid) {
			// System.out.println("valid!");
			for (Page page : pageList) {
//				System.out.println("valid page to write: "+page);c
				if (!(page.getContent().equals("datagem") && page.getDebug().equals("datagem"))) {
					context.write(new AvroKey<Page>(page), NullWritable.get());
				}
			}
		}
		// context.write(new AvroKey<CharSequence>("Page Count"), new
		// AvroValue<Integer>(sum));
	}
}