package mongo_query_test.mongo_query_test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.bson.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoDBTester {
	
	MongoDBConnection mongo = null;
	MongoClient client = null;
	String database	=	"Employee";
	String collection	=	"Emp";
	
	public MongoDBTester(){
		mongo = new MongoDBConnection();		
		client = mongo.connector();
	}
	
	public void tester() throws ParseException {
		
		if ( client != null ) {
			System.out.println("Connection successful!");
			MongoDatabase db = client.getDatabase(database);
			MongoCollection<Document> coll = db.getCollection(collection);
						
			String dt = "1989-06-10T00:00:00.000+0000";
			
			Date dayStart = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX").parse(dt);
			Date dayEnd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX").parse(dt+1);
			
			BasicDBObject dbo = new BasicDBObject("DOB", new BasicDBObject("$gt", dayStart))
								.append("DOB", new BasicDBObject("$lte", dayEnd));
			
			FindIterable<Document> doc = coll.find(dbo);
			
			for (Document dox : doc) {
				System.out.println(dox.toJson());
				System.out.println("Salary: "+dox.get("Sal"));
			}		
			
			
		}else {
			System.out.println("Connection refused!");
		}
	}

}
