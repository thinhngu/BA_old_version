package dataprocessing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;

import org.bson.Document;
import org.json.JSONObject;
import org.json.XML;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import com.mongodb.client.model.InsertManyOptions;

import analytics.Analytics;
import analytics.MongoQueries;

public class MySqlToMongoConnector {

	private static String mysqlpwd;
	private static int correctcounter = 0;
	private static int failedcounter = 0;

	private static MongoQueries mongodb;


	private final Logger logger = LoggerFactory.getLogger(MySqlToMongoConnector.class);



	public MySqlToMongoConnector() {
		this.mongodb = new MongoQueries();
	}




	private void loadDescriptionFilesToMongo(int limit, int offset) {


		//String query = "SELECT distinct links FROM crawler.all_file limit 1000;" ;
		String query = "SELECT distinct links FROM crawler.all_file limit "+ limit + " offset "+ offset + ";" ;


		try {

			Connection conn = DriverManager.getConnection("jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306", "sba", mysqlpwd);
			Statement stmt = conn.createStatement() ;
			ResultSet rs;

			//List<Document> documents = new ArrayList<Document>();

			if (stmt.execute(query)) {
				rs = stmt.getResultSet();

				while (rs.next()) {
					String url = rs.getString(1);
					logger.info("Found link in MySQL: " + url);

					url = cleanUrl(url);
					String document = getHttpDocument(url);
					try {
						if (document != null) {
							//sendDocumentToMongo(document, url);

							if (url.endsWith(".json")) {
								JSONObject json = new JSONObject(document);
								//								documents.add(mongodb.jsonToDoc(url, json ));
								mongodb.insertJson(url, json);
							} else if (url.endsWith(".yaml")) {
								//								documents.add(mongodb.yamlToDoc(url, document));
								mongodb.insertYaml(url, document);
							} else if (url.endsWith(".xml")) {
								//								documents.add(mongodb.xmlToDoc(url, document));
								mongodb.insertXml(url, document);
							} else if (url.endsWith(".rdf")) {
								//								documents.add(mongodb.xmlToDoc(url, document));
								mongodb.insertXml(url, document);
								// TODO wrong method...
							}
							logger.info("Succeeded: " + ++correctcounter);
						} else {
							logger.warn("Didn't find a document");
						}
					} catch (Exception e) {
						logger.warn("Exception (inner) transmitting documents from MySQL: ", e);
						logger.warn("Failed: " + ++failedcounter);
						/* continue */
					}
				}
				rs.close();
			}

			stmt.close();
			conn.close();


			//sendBulkDocumentsToMongo(documents);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.warn("Exception (outer) transmitting documents from SQL to MongoDB: ", e);
		}

	}





	@SuppressWarnings("unchecked")
	private void sendBulkDocumentsToMongo(List<Document> documents) {

		if (mongodb == null) mongodb = new MongoQueries();

		InsertManyOptions options = new InsertManyOptions().bypassDocumentValidation(true);
		mongodb.getCollection().insertMany(documents, options);


	}





	private String cleanUrl(String url) {
		// https://github.com/APIs-guru/openapi-directory/blob/master/APIs/zuora.com/patch.yaml
		// https://rawgit.com/APIs-guru/openapi-directory/master/APIs/zuora.com/patch.yaml

		if (url.contains("github.com")) url = url.replace("github", "rawgit")
				.replace("/blob", "").replace("/blame", "").replace("/commits", "");

		logger.debug("Cleaned URL: " + url);

		return url;
	}





	private void sendDocumentToMongo(String document, String url) {

		if (mongodb == null) mongodb = new MongoQueries();

		if (url.endsWith(".json")) {
			mongodb.insertJson(url, new JSONObject(document));
		} else if (url.endsWith(".yaml")) {
			mongodb.insertYaml(url, document);
		} else if (url.endsWith(".xml")) {
			mongodb.insertXml(url, document);
		} else if (url.endsWith(".rdf")) {
			mongodb.insertXml(url, document);
			// TODO wrong method...
		}

		mongodb.close();

	}







	private String getHttpDocument(String link) {

		String document = "";

		try {

			URL url = new URL(link);
			//			InputStream is = url.openStream();
			//
			//			Scanner scanner = new Scanner(is, "UTF-8");
			//			document = scanner.useDelimiter("\\A").next();
			//			scanner.close();
			//			is.close();

			HttpURLConnection huc = (HttpURLConnection) url.openConnection();
		       HttpURLConnection.setFollowRedirects(false);
		       huc.setConnectTimeout(5 * 1000);
		       huc.setReadTimeout(5 * 1000);
		       huc.setRequestMethod("GET");
		       huc.setRequestProperty("User-Agent", "Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US; rv:1.9.1.2) Gecko/20090729 Firefox/3.5.2 (.NET CLR 3.5.30729)");
		       huc.connect();
		       InputStream input = huc.getInputStream();

				BufferedReader in = new BufferedReader(
						new InputStreamReader( input ));
				
			String line = in.readLine();
			while (line != null) {
				document += line + "\n";
				line = in.readLine();
			}
			
			in.close();

		} catch (MalformedURLException e) {
			logger.warn(link, e);
		} catch (IOException e) {
			logger.warn(link, e);
		} catch (NoSuchElementException e) {
			logger.warn(link, e);
		}

		return document;
	}





	public static void main(String[] args) {

		mysqlpwd = args[0];

		MySqlToMongoConnector connector = new MySqlToMongoConnector();

		for (int i = 0; i <= 160; i++) {
			connector.loadDescriptionFilesToMongo(100, 16000+100*i);
		}

		mongodb.close();
	}
}
