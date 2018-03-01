package analytics;


import java.io.IOException;
import java.rmi.UnknownHostException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.bson.json.JsonReader;
import org.json.JSONObject;
import org.json.XML;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoException;
import com.mongodb.util.JSON;

public class MongoQueries {

	private MongoClient mongoClient;
	private DBCollection crawlercollection;


	public MongoQueries() {
	    MongoClientOptions.Builder options_builder = new MongoClientOptions.Builder();
	    options_builder.maxConnectionIdleTime(30000);
	    MongoClientOptions options = options_builder.build();
	    mongoClient = new MongoClient ("aifb-ls3-vm1.aifb.kit.edu:27017", options);

		//mongoClient = new MongoClient(new MongoClientURI("mongodb://aifb-ls3-vm1.aifb.kit.edu:27017"));
		//@SuppressWarnings("deprecation")
		//Mongo mongo = new Mongo("aifb-ls3-vm1.aifb.kit.edu", 27017);

		@SuppressWarnings("deprecation")
		DB db = mongoClient.getDB("crawler");
//		DB db = mongo.getDB("mongodb");

		// get a single collection
		this.crawlercollection = db.getCollection("apidescriptions");

	}

	
	public void close() {
		this.crawlercollection.drop();
		this.mongoClient.close();
	}


	
	public void execute(DBObject key, DBObject value) {


		BasicDBObject allQuery = new BasicDBObject();
		BasicDBObject fields = new BasicDBObject();
		fields.put("name", 6);

		//DBCursor cursor = this.crawlercollection.find(allQuery, fields);
		DBCursor cursor = this.crawlercollection.find(key, value);
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}




	public void insertJson(String url, JSONObject json) {

		DBObject data = BasicDBObject.parse(json.toString());
		data.put("_id", url);
		
		this.crawlercollection.insert(data);
	}
	


	public void insertXml(String url, String xml) {
		
		JSONObject json = XML.toJSONObject(xml);		
		DBObject data = BasicDBObject.parse(json.toString());
		data.put("_id", url);
		
		this.crawlercollection.insert(data);
	}


	public void insertYaml(String url, String yaml) {
			
		DBObject data = BasicDBObject.parse(convertYamlToJson(yaml));
		data.put("_id", url);
		
		this.crawlercollection.insert(data);
	}
	
	
	/**
	 * @author https://stackoverflow.com/users/446554/cory-klein
	 * 
	 * @param yaml
	 * @return
	 */
	private String convertYamlToJson(String yaml) {
		
		String json = null;

	    ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
	    Object obj;
		try {
			obj = yamlReader.readValue(yaml, Object.class);
		    ObjectMapper jsonWriter = new ObjectMapper();
		    json = jsonWriter.writeValueAsString(obj);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	    return json;
	}





	/**
	 * Java MongoDB : Query document
	 *
	 * @author mkyong
	 *
	 */
	public static void insertDummyDocuments(DBCollection collection) {

		List<DBObject> list = new ArrayList<DBObject>();

		Calendar cal = Calendar.getInstance();

		for (int i = 1; i <= 5; i++) {

			BasicDBObject data = new BasicDBObject();
			data.append("number", i);
			data.append("name", "mkyong-" + i);
			// data.append("date", cal.getTime());

			// +1 day
			cal.add(Calendar.DATE, 1);

			list.add(data);

		}

		collection.insert(list);

	}


	/**
	 * Java MongoDB : Query document
	 *
	 * @author mkyong
	 *
	 */
	public static void main(String[] args) {

		try {

			Mongo mongo = new Mongo("aifb-ls3-vm1.aifb.kit.edu", 27017);
			DB db = mongo.getDB("yourdb");

			// get a single collection
			DBCollection collection = db.getCollection("dummyColl");

			insertDummyDocuments(collection);

			System.out.println("1. Find first matched document");
			DBObject dbObject = collection.findOne();
			System.out.println(dbObject);

			System.out.println("\n1. Find all matched documents");
			DBCursor cursor = collection.find();
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}

			System.out.println("\n1. Get 'name' field only");
			BasicDBObject allQuery = new BasicDBObject();
			BasicDBObject fields = new BasicDBObject();
			fields.put("name", 1);

			DBCursor cursor2 = collection.find(allQuery, fields);
			while (cursor2.hasNext()) {
				System.out.println(cursor2.next());
			}

			System.out.println("\n2. Find where number = 5");
			BasicDBObject whereQuery = new BasicDBObject();
			whereQuery.put("number", 5);
			DBCursor cursor3 = collection.find(whereQuery);
			while (cursor3.hasNext()) {
				System.out.println(cursor3.next());
			}

			System.out.println("\n2. Find where number in 2,4 and 5");
			BasicDBObject inQuery = new BasicDBObject();
			List<Integer> list = new ArrayList<Integer>();
			list.add(2);
			list.add(4);
			list.add(5);
			inQuery.put("number", new BasicDBObject("$in", list));
			DBCursor cursor4 = collection.find(inQuery);
			while (cursor4.hasNext()) {
				System.out.println(cursor4.next());
			}

			System.out.println("\n2. Find where 5 > number > 2");
			BasicDBObject gtQuery = new BasicDBObject();
			gtQuery.put("number", new BasicDBObject("$gt", 2).append("$lt", 5));
			DBCursor cursor5 = collection.find(gtQuery);
			while (cursor5.hasNext()) {
				System.out.println(cursor5.next());
			}

			System.out.println("\n2. Find where number != 4");
			BasicDBObject neQuery = new BasicDBObject();
			neQuery.put("number", new BasicDBObject("$ne", 4));
			DBCursor cursor6 = collection.find(neQuery);
			while (cursor6.hasNext()) {
				System.out.println(cursor6.next());
			}

			System.out.println("\n3. Find when number = 2 and name = 'mkyong-2' example");
			BasicDBObject andQuery = new BasicDBObject();

			List<BasicDBObject> obj = new ArrayList<BasicDBObject>();
			obj.add(new BasicDBObject("number", 2));
			obj.add(new BasicDBObject("name", "mkyong-2"));
			andQuery.put("$and", obj);

			System.out.println(andQuery.toString());

			DBCursor cursor7 = collection.find(andQuery);
			while (cursor7.hasNext()) {
				System.out.println(cursor7.next());
			}

			System.out.println("\n4. Find where name = 'Mky.*-[1-3]', case sensitive example");
			BasicDBObject regexQuery = new BasicDBObject();
			regexQuery.put("name",
					new BasicDBObject("$regex", "Mky.*-[1-3]")
					.append("$options", "i"));

			System.out.println(regexQuery.toString());

			DBCursor cursor8 = collection.find(regexQuery);
			while (cursor8.hasNext()) {
				System.out.println(cursor8.next());
			}

			collection.drop();

			System.out.println("Done");

		} catch (MongoException e) {
			e.printStackTrace();
		}

	}


}