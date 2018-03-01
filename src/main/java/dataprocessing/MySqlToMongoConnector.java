package dataprocessing;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import analytics.Analytics;
import analytics.MongoQueries;

public class MySqlToMongoConnector {



	private final Logger logger = LoggerFactory.getLogger(MySqlToMongoConnector.class);


	private void loadDescriptionFilesToMongo() {


		//String query = "SELECT distinct links FROM crawler.all_file limit 1000;" ;
		String query = "SELECT distinct links FROM crawler.all_file limit 1000 offset 1000;" ;


		Connection conn;
		try {

			conn = DriverManager.getConnection("jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306", "sba", "Welcome");
			Statement stmt = conn.createStatement() ;
			ResultSet rs;
			if (stmt.execute(query)) {
				rs = stmt.getResultSet();
				int correctcounter = 0;
				int failedcounter = 0;

				while (rs.next()) {
					String url = rs.getString(1);
					logger.debug("Found link in MySQL: " + url);
					
					url = cleanUrl(url);
					String document = getHttpDocument(url);
					try {
						if (document != null) sendDocumentToMongo(document, url);
						logger.info("Succeeded: " + ++correctcounter);
					} catch (Exception e) {
						logger.warn("Exception (inner) transmitting documents from MySQL to MongoDB: ", e);
						logger.warn("Failed: " + ++failedcounter);
						/* continue */
					}
				}
			}

			conn.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.warn("Exception (outer) transmitting documents from SQL to MongoDB: ", e);
		}

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

		MongoQueries mongodb = new MongoQueries();

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

		String document = null;

		try {

			URL url = new URL(link);
			InputStream is = url.openStream();
			document = new Scanner(is, "UTF-8").useDelimiter("\\A").next();
			is.close();

		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return document;
	}





	public static void main(String[] args) {
		MySqlToMongoConnector connector = new MySqlToMongoConnector();
		connector.loadDescriptionFilesToMongo();
	}
}
