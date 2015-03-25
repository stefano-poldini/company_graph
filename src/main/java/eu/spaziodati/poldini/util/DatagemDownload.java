package eu.spaziodati.poldini.util;

import java.io.File;
import java.net.URL;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;

import eu.spaziodati.poldini.avro.Page;

public class DatagemDownload {


	public static void main(String[] args) throws Exception {

		// call dandelion service to obtain the maximum number of records in the
		// datagem (under the json field "count")
		ObjectMapper mapper = new ObjectMapper();
		String testUrl = "https://api.dandelion.eu/datagems/v2/SpazioDati/internet-data-gathering/data?$limit=0&$offset=0&$app_id="+Keys.APP_ID+"$app_key="+Keys.APP_KEY;
		JsonNode rootNode = mapper.readValue(new URL(testUrl), JsonNode.class);
		JsonNode count = rootNode.get("count");
		// get all the unique urls in dandelion datagem
		String url = "https://api.dandelion.eu/datagems/v2/SpazioDati/internet-data-gathering/data?$select=DISTINCT+website&$limit="
				+ count.getIntValue() + "&$offset=0&$app_id="+Keys.APP_ID+"$app_key="+Keys.APP_KEY;

		// get an instance of the json parser from the json factory
		JsonFactory factory = new JsonFactory();
		JsonParser parser = factory.createJsonParser(new URL(url));

		File file = new File(Configuration.DATAGEM_AVRO_OUTPUT_FILE);
		DatumWriter<Page> userDatumWriter = new SpecificDatumWriter<Page>(Page.class);
		DataFileWriter<Page> dataFileWriter = new DataFileWriter<Page>(userDatumWriter);
		dataFileWriter.create(Page.getClassSchema(), file);
		Page page;
		// continue parsing the token till the end of input is reached
		while (!parser.isClosed()) {
			// get the token
			JsonToken token = parser.nextToken();
			// if its the last token then we are done
			if (token == null)
				break;
			// we want to look for a field that says items
			if (JsonToken.FIELD_NAME.equals(token) && "items".equals(parser.getCurrentName())) {
				// we are entering the items now. The first token should be
				// start of array
				token = parser.nextToken();
				if (!JsonToken.START_ARRAY.equals(token)) {
					// bail out
					break;
				}
				// each element of the array is an album so the next token
				// should be {
				token = parser.nextToken();
				if (!JsonToken.START_OBJECT.equals(token)) {
					break;
				}
				// we are now looking for a field that says "website". We
				// continue looking till we find all such fields. This is
				// probably not a best way to parse this json, but this will
				// suffice for this example.
				while (true) {
					token = parser.nextToken();
					if (token == null)
						break;
					if (JsonToken.FIELD_NAME.equals(token) && "website".equals(parser.getCurrentName())) {
						token = parser.nextToken();
						// creates a Page with the url coming from datagem list
						// and all the rest null
						page = new Page(parser.getText(), "datagem", "datagem");
						dataFileWriter.append(page);
					}
				}
			}
		}
		dataFileWriter.close();
		// jsonUrls.openConnection().getInputStream();
		// String json=getContent(jsonUrls);
		 System.out.println("file downloaded to: "+Configuration.DATAGEM_AVRO_OUTPUT_FILE);
	}
}
