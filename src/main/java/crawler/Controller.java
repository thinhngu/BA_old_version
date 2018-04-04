package crawler;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Consumer;

import javax.net.ssl.HttpsURLConnection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class Controller {


	private static String bing_apikey;
	private static Logger logger = LoggerFactory.getLogger(Controller.class);


	public static void main(String[] args) throws Exception {

		String rootFolder = "data/crawltest";
		int numberOfCrawlers = 16;

		CrawlConfig config = new CrawlConfig();

		config.setCrawlStorageFolder(rootFolder);
		config.setMaxPagesToFetch(1000000);
		config.setPolitenessDelay(20);
		config.setIncludeHttpsPages(true);
		config.setIncludeBinaryContentInCrawling(true);
		config.setProcessBinaryContentInCrawling(true);


		PageFetcher pageFetcher = new PageFetcher(config);
		RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
		RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
		CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);


		controller.addSeed("https://www.programmableweb.com/category/all/apis");
		controller.addSeed("https://rapidapi.com/");
		controller.addSeed("http://www.apiforthat.com/apis");
		controller.addSeed("https://sdks.io/");
		controller.addSeed("https://www.producthunt.com/#!/s/posts/api");
		controller.addSeed("https://apilist.fun/");

		try {
		bing_apikey = args[0];
		for (String url : (new Controller()).getBingUrls("web service api",50,0) ) { 
			controller.addSeed(url);
		}
		for (String url : (new Controller()).getBingUrls("web service api",50,50) ) { 
			controller.addSeed(url);
		}
		} catch (Exception e) {
			logger.error("Error during Bing API request: ", e);
		}

		controller.start(Crawler.class, numberOfCrawlers);


	}


	/**
	 * 
	 * Stelle Anfragen an die BING WEB SEARCH API und gebe Liste der Webseiten zurück.
	 * 
	 * Maximal 50 Einträge werden vom Server zurückgegeben, daher kann paging eingesetzt werden.
	 * 
	 */
	private Iterable<String> getBingUrls(String query, int count, int offset) throws IOException {

		List<String> urls = new ArrayList<String>();

		String host = "https://api.cognitive.microsoft.com";
		String path = "/bing/v7.0/search";
		String subscriptionKey = this.bing_apikey; 
		//String customConfigId = "YOUR-CUSTOM-CONFIG-ID";

		// construct URL of search request (endpoint + query string)
        URL url = new URL(host + path + "?count=" + count + "&offset=" + offset + "&q=" +  URLEncoder.encode(query, "UTF-8"));
        HttpsURLConnection connection = (HttpsURLConnection)url.openConnection();
        connection.setRequestProperty("Ocp-Apim-Subscription-Key", subscriptionKey);

        // receive JSON body
        InputStream stream = connection.getInputStream();
        String response = new Scanner(stream).useDelimiter("\\A").next();

        // construct result object for return
        SearchResults results = new SearchResults(new HashMap<String, String>(), response);

        // extract Bing-related HTTP headers
        Map<String, List<String>> headers = connection.getHeaderFields();
        for (String header : headers.keySet()) {
            if (header == null) continue;      // may have null key
            if (header.startsWith("BingAPIs-") || header.startsWith("X-MSEdge-")) {
                results.relevantHeaders.put(header, headers.get(header).get(0));
            }
        }
 
        stream.close();
		
		for (String key : results.relevantHeaders.keySet()) {
			String value = results.relevantHeaders.get(key);
		}
		
		logger.info(prettify(results.jsonResponse));
		
		
		JsonParser parser = new JsonParser();
        JsonObject json = parser.parse(results.jsonResponse).getAsJsonObject();
        JsonObject webpages= (JsonObject) json.get("webPages");
        JsonArray values = (JsonArray) webpages.get("value");
        
        values.forEach(new Consumer<JsonElement>() {

			@Override
			public void accept(JsonElement t) {
				urls.add(((JsonObject) t).get("url").getAsString());
			}
        	
		});


		return urls;
	}

	// pretty-printer for JSON; uses GSON parser to parse and re-serialize
	public static String prettify(String json_text) {
		JsonParser parser = new JsonParser();
		JsonObject json = parser.parse(json_text).getAsJsonObject();
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		return gson.toJson(json);
	}

	// Container class for search results encapsulates relevant headers and JSON data
	class SearchResults{
		HashMap<String, String> relevantHeaders;
		String jsonResponse;
		SearchResults(HashMap<String, String> headers, String json) {
			relevantHeaders = headers;
			jsonResponse = json;
		}

	}

}
