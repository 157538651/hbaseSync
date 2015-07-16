package sync.mongodb;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import junit.framework.Assert;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import sync.vo.User;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.DBTCPConnector;
import com.mongodb.Mongo;
import com.mongodb.WriteResult;

public class MongodbTest {
	Mongo m;
	@SuppressWarnings("deprecation")
	DBTCPConnector conn;
	private DB db;
	@SuppressWarnings("deprecation")
	@Test
    public void testMongo() throws UnknownHostException {
        String host = "192.168.41.160";
        m = new Mongo(host, 27017);
    }
	
	 @SuppressWarnings("deprecation")
	@Test
	public void testConntect() throws Exception {
	        testMongo();
	        System.out.println("Mongo: " + m);
	        conn = m.getConnector();
	        System.out.println("DBTCPConnector: " + conn.getServerAddressList());
	}
	 @Test
	public void testDB() throws Exception {
		testMongo();
		testConntect();

		// 获取admin的数据库
		db = m.getDB("root");
		System.out.println("DB=" + db);
		@SuppressWarnings("deprecation")
		boolean auth = db.authenticate("root", "root".toCharArray());
		System.out.println("auth=" + auth);
	}
	 
	@Test
	public void testGetAll() throws Exception {
		testDB();
		// 获取db里面的collection(表)
		Set<String> names = db.getCollectionNames();

		for (String name : names) {
			System.out.println("CollectionName: " + name);
			DBCollection coll = db.getCollection(name);
			System.out.println("CollectionCount=" + coll.count());

			DBCursor cursor = coll.find();
			while (cursor.hasNext()) {
				System.out.println("DBObject=" + cursor.next());
			}

			List<DBObject> objs = coll.getIndexInfo();
			for (DBObject obj : objs) {
				System.out.println("IndexInfo=" + obj);
			}
			System.out.println("==============");
		}
		// 获取表结果
	}
	 
	@Test
    public void testCollection() throws Exception {
        testDB();
        if (db.isAuthenticated()) {
            for (String coll : db.getCollectionNames()) {
                System.out.println("collection=" + coll);
            }
        }
    }
	

    @Test
    public void testInsert() throws Exception {
 
        testDB();
 
        if (db.isAuthenticated()) {
            DBCollection coll = db.getCollection("user");
            DBObject obj = new BasicDBObject();
            obj.put("name", "chenpengfei");
            obj.put("age", 25);
            obj.put("md5", "546466sfsddfsd");
            WriteResult wr = coll.insert(obj);
            System.out.println("WriteResult=" + wr);
        }
    }
    
    @Test
    public void testInsertObj() throws Exception {
        testDB();
        if (db.isAuthenticated()) {
            DBCollection coll = db.getCollection("test");
            DBObject obj = new BasicDBObject();
            User u = new User();
            u.setId("00001");
            u.setName("Heli");
            u.setAge(25+"");
            u.setSex("女");
            obj.put("pepole", u);
            WriteResult wr = coll.insert(obj);
            System.out.println("WriteResult=" + wr);
        }
    }
   
    @Test
    public void testRemove() throws Exception {
        testDB();
        if (db.isAuthenticated()) {
            DBCollection coll = db.getCollection("user");
            DBObject obj = new BasicDBObject();
            // obj.put("name", "gaojie1");
            obj.put("age", 110);
         
            System.out.println("WriteResult=" + coll.remove(obj));
        }
    }
    
    @Test
    public void testUpdate() throws Exception {
        testDB();
 
        if (db.isAuthenticated()) {
            DBCollection coll = db.getCollection("test");
            DBObject obj = new BasicDBObject();
            obj.put("name", "gaojie10");
            obj.put("age", 110);
 
            DBObject upObj = new BasicDBObject();
            upObj.put("name", "gaojie10");
 
            // upObj.put("age", 110);
 
            System.out.println("WriteResult=" + coll.update(obj, upObj));
        }
    }
    
    @Test
    public void testQuery() throws Exception {
        testDB();
 
        if (db.isAuthenticated()) {
            DBCollection coll = db.getCollection("test");
            DBObject obj = new BasicDBObject();
            // obj.put("name", "gaojie00");
            obj.put("age", 110);
 
            DBCursor cursor = coll.find(obj);
            while (cursor.hasNext()) {
                System.out.println("DBObject=" + cursor.next());
            }
        }
    }

}	
