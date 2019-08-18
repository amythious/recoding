package mongo_query_test.mongo_query_test;

import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;

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
						
			
			/*****************************	PART-1: Date Match	*************************************************/
//			Date matches
			String dt = "1989-06-10T00:00:00.000+0000";
			
			Date dayStart = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX").parse(dt);
			Date dayEnd = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXX").parse(dt+1);
			
			BasicDBObject dbo = new BasicDBObject("DOB", new BasicDBObject("$gt", dayStart))
								.append("DOB", new BasicDBObject("$lte", dayEnd));
			
			FindIterable<Document> docs = coll.find(dbo);
								
			for (Document dox : docs) {
				System.out.println(dox.toJson());
			}
			
			/*****************************	PART-2: Get max record	*************************************************/
			
			BasicDBObject	bdbo	=	new BasicDBObject();			
			dbo.append("Age", new BasicDBObject("$gt",30));	// mention cusip here			
			FindIterable<Document> doc = coll.find(bdbo);						
			int max = 0;
			String p = null; 
			
			for (Document dox : doc) {
//				System.out.println(dox.toJson());
				
				if ( dox.getObjectId("_id").getTimestamp() > max ) {
					max = dox.getObjectId("_id").getTimestamp();
					p = dox.getObjectId("_id").toHexString();
					
				}
				
			}		
			
			bdbo.put("_id", new ObjectId(p));
			doc = coll.find(bdbo);
			Document dox = doc.first();
			System.out.println("Max Sal: "+dox.get("Sal"));	// max record val
			
			
			
		}else {
			System.out.println("Connection refused!");
		}
	}

	public void newTester() {
		if ( client != null ) {
			System.out.println("Connection successful!");
			MongoDatabase db = client.getDatabase(database);
			MongoCollection<Document> coll = db.getCollection(collection);
			
			AggregateIterable<Document> docs = coll.aggregate(
					Arrays.asList(
							Aggregates.match(Filters.eq("Dept", "IT")),
							Aggregates.match(Filters.lte("Age", 30)),
							Aggregates.group("EmpId", Accumulators.max("id", "$_id"))							
							)					
					);
			
			for(Document doc : docs) {
				System.out.println(doc.toJson());
			}
		}
	}
}
