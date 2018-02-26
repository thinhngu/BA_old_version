package analytics;

import org.json.JSONObject;

import com.mongodb.BasicDBObject;


public class Analytics {

	public static void main(String[] args) {
		
		MongoQueries queries = new MongoQueries();
		
		JSONObject swagger = new JSONObject("{\"swagger\":\"2.0\",\"info\":{\"description\":\"This is a sample server Petstore server.  You can find out more about Swagger at [http://swagger.io](http://swagger.io) or on [irc.freenode.net, #swagger](http://swagger.io/irc/).  For this sample, you can use the api key `special-key` to test the authorization filters.\"} }");
		//queries.insertJson("http://petstore.swagger.io/v2/swagger.json".replace(".", ""), swagger);
		
		queries.execute(new BasicDBObject(), new BasicDBObject());

	}

}
