package eu.spaziodati.poldini.mapreduce.page_filter;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import eu.spaziodati.poldini.avro.Page;

public class PageFilterMapper extends Mapper<AvroKey<Page>, NullWritable, Text, AvroValue<Page>> {
	@Override
	public void map(AvroKey<Page> key, NullWritable value, Context context) throws IOException, InterruptedException {
		Page page = key.datum();
		String url = page.getUrl().toString();
		if (url != null && !url.equals("")) {
			try {
				URI uri=new URI(url);
				if (!uri.isAbsolute())
					uri=new URI("http://"+url);			
				String host=uri.getHost();
//				if (host.contains("enerxia")){
//					System.out.println(page);
//				}
				context.write(new Text(host), new AvroValue<Page>(page) );
//				context.write(page, page);
//				System.out.println(url);

			} catch (URISyntaxException e) {
				context.getCounter("ERRORS", "URL_SINTAX_ERROR").increment(1);
//				e.printStackTrace();
			}catch(Exception e1){
//				Cannot create a Text class because the url is not in UTF8 (i.e. etancoçdnet.it)
				context.getCounter("ERRORS", "URL_SINTAX_ERROR").increment(1);
//				e1.printStackTrace();
			}
			
		}
	}
}
