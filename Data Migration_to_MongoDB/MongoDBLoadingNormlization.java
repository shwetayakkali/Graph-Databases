
import java.sql.*;
import java.lang.*;
import com.mongodb.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.eq;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bson.Document;
import org.bson.conversions.Bson;
import static com.mongodb.client.model.Filters.eq;

public class MongoDBLoadingNormlization {
    private static MongoDatabase mongo_db = null;
    private static MongoClient mongo = null;
    static Connection mysql_con = null;
    static Statement stmt = null;
    static String database_url = "jdbc:mysql://localhost:3306/Movies";
    static String username = "root";
    static String  password = "root";
    static ResultSet result = null;
    static PreparedStatement prepstmt = null;
    static MongoCollection<Document> collection = null;
    
    
    private static void createCollections(){ 
        mongo_db = mongo.getDatabase("imdb_normalized");
        MongoCollection<Document> collection = mongo_db.getCollection("actors");
        collection.drop(); 
        mongo_db.createCollection("actors");
        
        collection = mongo_db.getCollection("directors");
        collection.drop(); 
        mongo_db.createCollection("directors");
        
        collection = mongo_db.getCollection("genres");
        collection.drop(); 
        mongo_db.createCollection("genres");
        
        collection = mongo_db.getCollection("roles");
        collection.drop(); 
        mongo_db.createCollection("roles");
        
        /*collection = mongo_db.getCollection("moviestemp");
        collection.drop(); 
        mongo_db.createCollection("moviestemp");*/
        
        collection = mongo_db.getCollection("movies");
        collection.drop(); 
        mongo_db.createCollection("movies");
    
    }
    private static void mongoConnection(){
        mongo = new MongoClient( "localhost" , 27017 ); 
        System.out.println("Connected to MongoDB"); 
    
    }
    
    private static void mySQLConnection() throws ClassNotFoundException, SQLException{
        Class.forName("com.mysql.jdbc.Driver");  
        mysql_con = DriverManager.getConnection(database_url,username,password);
        System.out.println("Connected to MySQL"); 

    }
   
    
    private static List<Document> createList(){
        
        List<Document> doc_list = new ArrayList<>(id_map.values());
        
        return doc_list;
    }
    
    private static String convertToCommaSeparatedValues(){
        
        StringBuffer cs_str_values = new StringBuffer();
        for(int key : id_map.keySet()){
            Document current = id_map.get(key);
            String str = current.getInteger("_id") + ", ";
            cs_str_values.append(str);
        }
        String cs_str_new = new String(cs_str_values.substring(0, cs_str_values.length()-2));
        //cs_str_values = null;
        return cs_str_new;
    }
    
    private static void computeDocument(int movieid, int actorid, int directorid, int genreid) throws SQLException{
        
        Document current = id_map.get(movieid);
        
        ArrayList<Integer> list = (ArrayList<Integer>) current.get("actorid");
        if(!list.contains(actorid) && actorid!= -1){
            list.add(actorid);
            current.replace("actorid",list);
            
        }

        list = (ArrayList<Integer>) current.get("directorid");
        if(!list.contains(directorid) && directorid != -1){ 
            list.add(directorid);
            current.replace("directorid",list);
          
        }

        list = (ArrayList<Integer>) current.get("genreid");
        if(!list.contains(genreid) && genreid != -1){
            list.add(genreid);
            current.replace("genreid",list);
        }

        id_map.put(movieid,current);
       
    }
    
    private static HashMap <Integer,Document> id_map = new HashMap<>();
    
    private static void updateDocuments() throws SQLException{
        
        String cs_str_values = convertToCommaSeparatedValues();
        //String query = "select m.id, r.actorid, db.directorid, mg.genreid from roles r join movies m on r.movieid = m.id join directedby db on db.movieid = m.id join moviegenres mg on mg.movieid = m.id \n" +
                //"where m.id in ("+cs_str_values+")";
        
        String query = "Select movieid, actorid,directorid,genreid From\n" +
                       "(select movieid, actorid, null as directorid, null as genreid From roles Where movieid in (" + cs_str_values + ") \n" +
                       "Union all\n" +
                       "Select movieid, null as actorid, directorid,null as genreid  from directedby Where movieid in (" + cs_str_values + ")\n" +
                       "union all\n" +
                       "select movieid, null as actorid ,null as directorid ,genreid from moviegenres Where movieid in (" + cs_str_values + ") ) temp;";
        
        
        Statement stmt_new = mysql_con.createStatement();
        ResultSet result_new = stmt_new.executeQuery(query);
        
        stmt_new.setFetchSize(20000);

        //int ctr_new = 0;
        while(result_new.next()){
            
            int movieid = result_new.getInt(1);
            int actorid = result_new.getInt(2);
            if(result_new.wasNull()){
                actorid = -1;
            }
            int directorid = result_new.getInt(3);
            if(result_new.wasNull()){
                directorid = -1;
            }
            int genreid = result_new.getInt(4);
            if(result_new.wasNull()){
                genreid = -1;
            }
            
            computeDocument(movieid, actorid, directorid, genreid);
            
        }
               
        collection.insertMany(createList());
        
        result_new.close();
        stmt_new.close();
    
    }
    
    
    
    
    private static void populate_movies() throws SQLException{
        
        int ctr = 0;
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("movies");
        
        result = stmt.executeQuery("use Movies;");
        String query = "select id, title, year from movies;";
        result = stmt.executeQuery(query);
        stmt.setFetchSize(5000);
        
        //HashMap<Integer,Document> id_map = new HashMap<>();
        
        while(result.next()){
            int movieid = result.getInt(1);
            String title = result.getString(2);
            int year = result.getInt(3);
            
            Document document = new Document();
            document.put("_id",movieid);
            document.put("title",title);
            document.put("year",year);
            document.put("actorid",new ArrayList<Integer>());
            document.put("directorid",new ArrayList<Integer>());
            document.put("genreid",new ArrayList<Integer>());
            
            if(!id_map.containsKey(movieid)){
                id_map.put(movieid,document);
                ctr ++;
            }
            
            
            if(ctr == 5000){
               
                updateDocuments();
                //System.out.println(" Added "+ctr);
                ctr = 0;
                id_map.clear();
            }
            
        }
        if(ctr > 0){
            updateDocuments();
            ctr = 0;
            id_map.clear();
            System.out.println("All records added to Movies, count = 839055");
           
        }
        result.close();
        stmt.close();
    }
    
    /*
    private static void populate_moviesFinal() throws SQLException{
        //List<Document> document_list = new ArrayList<>();
        int ctr  = 0;
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("moviestemp");
  
        result = stmt.executeQuery("use Movies;");
        String query = "select m.id from movies m;";
        result = stmt.executeQuery(query);
            
        stmt.setFetchSize(10000);
        HashMap<Integer,Document> id_map = new HashMap<>();
        
        while(result.next()){
            int movieid = result.getInt(1);
            FindIterable<Document> findIterable = collection.find(eq("_id", movieid));
            
            for(Document current : findIterable){
                id_map.put(movieid,current);
            }
            
            Statement stmt_temp = mysql_con.createStatement();
            ResultSet result_temp = stmt_temp.executeQuery("use Movies;");
            result_temp = stmt_temp.executeQuery("select directorid from directedby where movieid = "+movieid+";");
            List<Integer> directorids = new ArrayList<>();
            while(result_temp.next()){
                directorids.add(result_temp.getInt(1));            
            }
            
            Document current = id_map.get(movieid);
            current.put("directorid",directorids);           
            //directorids.clear();
            
            stmt_temp = mysql_con.createStatement();
            result_temp = stmt_temp.executeQuery("select actorid from roles where movieid = "+movieid+";");
            List<Integer> actorids = new ArrayList<>();
            while(result_temp.next()){
                actorids.add(result_temp.getInt(1));            
            }
            
            current.put("actorid",actorids);           
            //actorids.clear();
            
            stmt_temp = mysql_con.createStatement();
            result_temp = stmt_temp.executeQuery("select genreid from moviegenres where movieid = "+movieid+";");
            List<Integer> genreids = new ArrayList<>();
            while(result_temp.next()){
                genreids.add(result_temp.getInt(1));            
            }
            
            current.put("genreid",genreids);           
            //genreids.clear();
            
            id_map.put(movieid,current);
            ctr ++;
            
            if(ctr == 10000){
                //System.out.println("before "+id_map.size());
                MongoCollection<Document> collection_temp = mongo_db.getCollection("movies");
                collection_temp.insertMany(createList(id_map));
                System.out.println("Added "+ctr);
                id_map.clear();
                ctr = 0;
            }
            
        }
        if(ctr > 0){
            //System.out.println("before "+id_map.size());
            MongoCollection<Document> collection_temp = mongo_db.getCollection("movies");
            collection_temp.insertMany(createList(id_map));
            System.out.println("All records in Movies Loaded");
            id_map.clear();
            ctr = 0;
        
        }
        
        
    
    }
    
    
    private static void populate_moviesTemp() throws SQLException{
        int ctr = 0;
        
        List<Document> document_list = new ArrayList<>();
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("moviestemp");
  
        result = stmt.executeQuery("use Movies;");
        //String query = "select id, title, year from movies;";
        String query = "select m.id,m.title,m.year from movies m;";
            
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        while(result.next()){
            Document document = new Document();
            int movieid = result.getInt(1);
            int year = result.getInt(3); 
            String title = result.getString(2); 
            document.put("_id",movieid);
            document.put("title",title);
            document.put("year",year);
            //document.put("directorid",new ArrayList<Integer>());
            //document.put("actorid",new ArrayList<Integer>());
            
            ctr ++;
            
            document_list.add(document);
            if(ctr == 10000){
                collection.insertMany(document_list);
                document_list.clear();
                //System.out.println("Rows Inserted"+ctr);
                ctr = 0;
            }   
        
        }
        if( ctr > 0){                
            collection.insertMany(document_list);
            document_list.clear();
            System.out.println("All movie ids, title, year inserted in movies ");
            ctr = 0;
        }
        
    }
    */
    
    private static void populate_rolesInMongo() throws SQLException{
        int ctr = 0;
        
        List<Document> document_list = new ArrayList<>();
        
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("roles");
  
        result = stmt.executeQuery("use Movies;");
        String query = "select movieid, actorid, role from roles;";
        
            
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        while(result.next()){
            Document document = new Document();
            int movieid = result.getInt(1);
            int actorid = result.getInt(2);
            String role = result.getString(3);
            
            document.put("movieid",movieid);
            document.put("actorid",actorid);
            document.put("role",role);
            
            ctr ++;
            
            document_list.add(document);
            if(ctr == 10000){
                collection.insertMany(document_list);
                document_list.clear();
                //System.out.println("Rows Inserted"+ctr);
                ctr = 0;
            }   
        
        }
        if( ctr > 0){                
            collection.insertMany(document_list);
            document_list.clear();
            System.out.println("All records added in Roles, count = 5713049 ");
            ctr = 0;
        }
        result.close();
        stmt.close();
    
    }
    
    
    private static void populate_genresInMongo() throws SQLException{
        int ctr = 0;
        
        List<Document> document_list = new ArrayList<>();
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("genres");
  
        result = stmt.executeQuery("use Movies;");
        String query = "select id, genre from genres;";
                    
        stmt.setFetchSize(7000);
        result = stmt.executeQuery(query);
        while(result.next()){
            Document document = new Document();
            int id = result.getInt(1);
            String genre = result.getString(2);
            
            document.put("_id",id);
            document.put("genre",genre);
            
            ctr ++;
            
            document_list.add(document);
            if(ctr == 7000){
                collection.insertMany(document_list);
                document_list.clear();
                //System.out.println("Rows Inserted"+ctr);
                ctr = 0;
            }   
        
        }
        if( ctr > 0){                
            collection.insertMany(document_list);
            document_list.clear();
            System.out.println("All records added in Genres, count = 161 ");
            ctr = 0;
        }
        result.close();
        stmt.close();
    
    }
    
    
    
    private static void populate_directorsInMongo() throws SQLException{
        int ctr = 0;
        
        List<Document> document_list = new ArrayList<>();
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("directors");
  
        result = stmt.executeQuery("use Movies;");
        String query = "select id, first, last from directors;";
        
            
        stmt.setFetchSize(7000);
        result = stmt.executeQuery(query);
        while(result.next()){
            Document document = new Document();
            int id = result.getInt(1);
            String first = result.getString(2);
            String last = result.getString(3);
            document.put("_id",id);
            document.put("first",first);
            document.put("last",last);
            ctr ++;
            
            document_list.add(document);
            if(ctr == 7000){
                collection.insertMany(document_list);
                document_list.clear();
                //System.out.println("Rows Inserted"+ctr);
                ctr = 0;
            }   
        
        }
        if( ctr > 0){                
            collection.insertMany(document_list);
            document_list.clear();
            System.out.println("All records added in Directors, size = 287771 ");
            ctr = 0;
        }
        result.close();
        stmt.close();
    }
    
    
    private static void populate_actorsInMongo() throws SQLException{
        int ctr = 0;
        
        List<Document> document_list = new ArrayList<>();
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("actors");
  
        result = stmt.executeQuery("use Movies;");
        String query = "select id, first, last from actors;";
        
            
        stmt.setFetchSize(7000);
        result = stmt.executeQuery(query);
        while(result.next()){
            Document document = new Document();
            int id = result.getInt(1);
            String first = result.getString(2);
            String last = result.getString(3);
            document.put("_id",id);
            document.put("first",first);
            document.put("last",last);
            ctr ++;
            
            document_list.add(document);
            if(ctr == 7000){
                collection.insertMany(document_list);
                document_list.clear();
                //System.out.println("Rows Inserted"+ctr);
                ctr = 0;
            }   
        
        }
        if( ctr > 0){                
            collection.insertMany(document_list);
            document_list.clear();
            System.out.println("All records added in Actors, count = 2214366 ");
            ctr = 0;
        }
        result.close();
        stmt.close(); 
           
    }
    
    
    
    
    
    public static void main(String args[]) throws ClassNotFoundException, SQLException{
        
        mongoConnection();
        mySQLConnection();
        createCollections();
        populate_actorsInMongo();
        populate_directorsInMongo();
        populate_genresInMongo();
        populate_rolesInMongo();
        
        populate_movies();
        
        
        
        
        
    }
}


 /*
    private static void populate_moviesInMongo() throws SQLException{
        
        HashMap<Integer,Document> id_map = new HashMap<>();
        int ctr = 0;
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("movies");
        
        result = stmt.executeQuery("use Movies;");
        String query = "select m.id, db.directorid, r.actorid from directedby db join movies m on m.id = db.movieid join roles r on r.movieid = m.id;";
               
        result = stmt.executeQuery(query);
        
        stmt.setFetchSize(10000);
        while(result.next()){
            int movieid = result.getInt(1);
            int directorid = result.getInt(2);
            int actorid = result.getInt(3);
            
            if(id_map.containsKey(movieid)){
                Document current = id_map.get(movieid);
                
                ArrayList<Integer> list = (ArrayList<Integer>)current.get("directorid");
                if(!list.contains(directorid)){
                    list.add(directorid);
                    current.put("directorid",list);

                }

                list = (ArrayList<Integer>)current.get("actorid");

                if(!list.contains(actorid)){
                    list.add(actorid);
                    current.put("actorid",list);
                }
                
                id_map.put(movieid, current);
                
            
            }
            else if(collection.find(eq("_id",movieid)) != null){
                Document current = collection.findOneAndDelete(new Document("_id", movieid));
                
                ArrayList<Integer> list = (ArrayList<Integer>)current.get("directorid");
                if(!list.contains(directorid)){
                    list.add(directorid);
                    current.put("directorid",list);

                }

                list = (ArrayList<Integer>)current.get("actorid");

                if(!list.contains(actorid)){
                    list.add(actorid);
                    current.put("actorid",list);
                }
                
                id_map.put(movieid, current);
            
            }

            else{
                
            
            }
        
        
        }
        
        
        
        
    
    
    }
    
    
    
    
    private static void populate_moviesDirectors() throws SQLException{
        
        //List<Document> document_list = new ArrayList<>();
        //int last_movie_id = -1;
        //Document last_document = null;
        
        int ctr = 0;
        
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("movies");
        
        result = stmt.executeQuery("use Movies;");
        String query = "select m.id, db.directorid, r.actorid from directedby db join movies m on m.id = db.movieid join roles r on r.movieid = m.id;";
               
        result = stmt.executeQuery(query);
        
        stmt.setFetchSize(10000);
        
        while(result.next()){
            
            int movieid = result.getInt(1);
            int directorid = result.getInt(2); 
            int actorid = result.getInt(3); 
            
            Document current = collection.findOneAndDelete(new Document("_id", movieid));
         
            ArrayList<Integer> list = (ArrayList<Integer>)current.get("directorid");
            if(!list.contains(directorid)){
                list.add(directorid);
                current.put("directorid",list);
                
            }
            
            list = (ArrayList<Integer>)current.get("actorid");
            
            if(!list.contains(actorid)){
                list.add(actorid);
                current.put("actorid",list);
                //System.out.println("The list of actors = "+list); 

            }
            
            //collection.insertOne(current);
            ctr ++;
            
        }
        System.out.println("Counter value = "+ctr);
    }
    */