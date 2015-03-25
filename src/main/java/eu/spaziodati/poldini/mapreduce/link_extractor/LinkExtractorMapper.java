package eu.spaziodati.poldini.mapreduce.link_extractor;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import eu.spaziodati.poldini.avro.Page;

public class LinkExtractorMapper extends Mapper<AvroKey<Page>, NullWritable, Text, Text> {
	private static boolean TEST = false;
	private static int TEST_COUNT = 3;

	private static final String HREF_REGEX = "a[href~=((^(http|https)://)|/|ww).*$]";
	private static final String[] INVALID_EXTENSIONS = { "css", "js" };
	// private static final String[] IMAGE_EXTENSIONS = { "png", "jpg", "jpeg",
	// "tiff", "gif", "bmp" };
	public static final String BLOCKQUOTE_REGEX = "blockquote";
	public static final String BLOCKQUOTE_PREFIX = "blockquote:";
	// p tags that contains at least one child
	// private static final String PAR_REGEX = "p:has(bloquote,a,img,p)";
	// p tags that contain some text, not only tags
	// private static final String PAR_REGEX_NOT_EMPTY = "p:matchesOwn(.)";
	public static final String PAR_PREFIX = "par";

	protected static enum MAPPERCOUNTER {
		NOT_RECOGNIZED_AS_HTML, HTML_PARSE_FAILURE, HTML_PAGE_TOO_LARGE, EXCEPTIONS, OUT_OF_MEMORY, PAGES_PROCESSED
	}

	protected static enum TAGS {
		A, BLOCKQUOTE
	}

	@Override
	protected void map(AvroKey<Page> key, NullWritable value,
			Mapper<AvroKey<Page>, NullWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		Page page = key.datum();
		try {
		String url = getValidUrl(page.getUrl().toString(), null);

		if (TEST) {
			if (TEST_COUNT != 0)
				TEST_COUNT--;
			else
				return;
		}
		// ensure sample instances have enough memory to parse HTML
		if (url.length() > (5 * 1024 * 1024)) {
			context.getCounter(MAPPERCOUNTER.HTML_PAGE_TOO_LARGE).increment(1);
			return;
		}
		Document doc = Jsoup.parse(page.getContent().toString());
		// System.out.println(page.getContent().toString());
//		System.out.println(doc.html());

		
			extractLinks(context, doc, url);
			context.write(new Text(url), new Text(""));
		} catch (URISyntaxException e) {
			context.getCounter(MAPPERCOUNTER.HTML_PARSE_FAILURE).increment(1);
			// e.printStackTrace();
			return;
		}
		context.getCounter(MAPPERCOUNTER.PAGES_PROCESSED).increment(1);
	}

	private void extractLinks(Context context, Document doc, String page_url) throws IOException, InterruptedException,
			URISyntaxException {

		Elements elements = doc.select(HREF_REGEX + " ," + BLOCKQUOTE_REGEX);
		String host = new URI(page_url).getHost();
		// System.out.println("--------------------------------");
		// System.out.println("host: " + host);
		for (int i = 0; i < elements.size(); i++) {
			Element child = elements.get(i);
//			 System.out.println("doc: " + doc.tagName() +
//			 "  child: " + child.tagName());
//			 System.out.println(child.outerHtml());

			TAGS tagName = null;
			String child_name = child.tagName();
			if (child_name.equals("a"))
				tagName = TAGS.A;
			else if (child_name.equals("blockquote"))
				tagName = TAGS.BLOCKQUOTE;

			switch (tagName) {
			case A:
				String href = child.attr("href").toLowerCase().trim();
				href = getValidUrl(href, host);
				if (href == null) {
					context.getCounter(MAPPERCOUNTER.HTML_PARSE_FAILURE).increment(1);
					return;
				}
//				System.out.println("valid target:" + href+" , source:"+page_url);

				context.write(new Text(href), new Text(page_url));

				break;

			case BLOCKQUOTE:
				String cite = child.attr("cite").toLowerCase().trim();
				cite = getValidUrl(cite, host);
				if (cite == null) {
					context.getCounter(MAPPERCOUNTER.HTML_PARSE_FAILURE).increment(1);
					return;
				}
				// System.out.println("valid citations:"+child.toString());
				context.write(new Text(cite), new Text(page_url));
				break;
			default:
				System.out.println("error, tag not recognized");
				context.getCounter(MAPPERCOUNTER.HTML_PARSE_FAILURE).increment(1);
				break;
			}
		}
		// System.out.println("--------------------------------");

	}

	private String getValidUrl(String href, String host) throws URISyntaxException {
		URI url = new URI(href);
		if (!url.isAbsolute()) {
			if (href.equals("/") || href.equals(""))
				return null;
			// System.out.println("relative url: " + href+"   host:"+host);
			if(href.startsWith("/"))
				href = host + href;
			else // example: www.foo.com
				href="http://"+href;
		}
		if (!validExtensions(href)) {
			// System.err.println("Avoid extension error: " + href);

			return null;
		}

		return href;
	};

	private boolean validExtensions(String href) {
		for (int i = 0; i < INVALID_EXTENSIONS.length; i++) {
			if (href.endsWith("." + INVALID_EXTENSIONS[i])) {
				// System.out.println(href);
				return false;
			}
		}
		return true;
	}
}
