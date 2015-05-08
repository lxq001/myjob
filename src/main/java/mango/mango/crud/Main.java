package mango.mango.crud;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.Cursor;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import com.mongodb.ParallelScanOptions;
import com.mongodb.WriteResult;

public class Main {

	public static void main(String[] args) throws UnknownHostException {
		MongoClient mongo = new MongoClient("127.0.0.1");
		DB db = mongo.getDB("foo");
		DBCollection collection = db.getCollection("user");
		BasicDBList obj = new BasicDBList();
		DBObject obj2 = new BasicDBObject();
		obj.put("address", "beijing");

		collection.insert(obj);
		collection.insert(obj2);
		DBObject o = collection.findOne();
		System.out.println(o.get("name"));
		System.out.println(collection.count());

		mongo.close();

	}

	MongoClient mongo = null;

	@Test
	public void testTour() throws UnknownHostException {
		mongo = new MongoClient("127.0.0.1");
		DB db = mongo.getDB("test");
		try {
			DBCollection collection = db.getCollection("testCollection");
			BasicDBObjectBuilder builder = new BasicDBObjectBuilder().append("name", "leo").append("address", new BasicDBObject().append("city", "bj").append("street", "tf"));
			WriteResult result = collection.insert(builder.get());
			System.out.println(result.isUpdateOfExisting());
			DBObject basicDBObject = collection.findOne();
			System.out.println(basicDBObject.toString());
			for (int i = 0; i < 100; i++) {
				collection.insert(new BasicDBObject("i", i));
			}
			//查询所有数据
			DBCursor cursor = collection.find();
			while (cursor.hasNext()) {
				DBObject o = cursor.next();
				System.out.println(o.toString());
				System.out.println("id-->" + o.get("_id"));
			}
			cursor.close();
			System.out.println("query---------------------");
			BasicDBObject query = new BasicDBObject("i", 77);
			query(collection, query);
			//query with contidion 条件查询
			query = new BasicDBObject("i", new BasicDBObject("$gte", 30));//new BasicDBObject("$gte", 30) >=
			query(collection, query);
			//查询数量
			int count = collection.find().count();
			System.out.println("count-->" + count);
			
			
			//批量操作
			BulkWriteOperation bulk = collection.initializeOrderedBulkOperation();//有序的批量操作，在第一个出现错误时挂起
			bulk.insert(new BasicDBObject("_id", 1));
			bulk.insert(new BasicDBObject("_id", 2));
			bulk.insert(new BasicDBObject("_id", 3));
			bulk.find(new BasicDBObject("_id", 1)).updateOne(new BasicDBObject("$set", new BasicDBObject("x", 2)));
			bulk.find(new BasicDBObject("_id", 2)).removeOne();
			bulk.find(new BasicDBObject("_id", 3)).replaceOne(new BasicDBObject("_id", 3).append("x", 4));
			BulkWriteResult bulkWriteResult = bulk.execute();
			System.out.println("Ordered bulk write result : " + bulkWriteResult);
			
			
			//用于并行集合扫描
			ParallelScanOptions parallelScanOptions = ParallelScanOptions.builder().batchSize(50).numCursors(3).build();
			//获得游标集合
			List<Cursor> cursors = collection.parallelScan(parallelScanOptions);
			for (Cursor pCursor : cursors) {
				while (pCursor.hasNext()) {
					System.out.println(pCursor.next());
				}
			}

		} finally {
			//释放资源
			db.dropDatabase();
			mongo.close();
		}

	}
	@Test
	public void testScramSha1Credentials(){
		
	}
	
	

	private void query(DBCollection collection, BasicDBObject query) {
		DBCursor cursor;
		cursor = collection.find(query);
		try {
			while (cursor.hasNext()) {
				DBObject o = cursor.next();
				System.out.println(o.toString());
				System.out.println("id-->" + o.get("_id"));
			}
		} finally {
			cursor.close();
		}
	}

}
