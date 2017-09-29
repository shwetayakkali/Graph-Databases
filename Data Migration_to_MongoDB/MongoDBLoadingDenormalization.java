
import java.sql.SQLException;
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

public class MongoDBLoadingDenormalization {
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
    private static HashMap <Integer,Document> id_map = new HashMap<>();
    
    
    
    /*private static Document computeInnerGenresDocument(Document inner_key, int genreid, String genre){
        /* if(inner_key == null){
            inner_key =  new Document();
            ArrayList<Integer> list = new ArrayList<>();
            if(!list.contains(genreid) && genreid != -1){
                list.add(genreid);
                inner_key.replace("genreid",list);
            }
            
            ArrayList<String> strlist = new ArrayList<>();
            if(!strlist.contains(genre) && !genre.equals("")){
                strlist.add(genre);
                inner_key.replace("genre",strlist);
            }
        }
        else{ 
            
            ArrayList<Integer> list = (ArrayList<Integer>) inner_key.get("genreid");
            if(!list.contains(genreid) && genreid != -1){
                list.add(genreid);
                inner_key.replace("genreid",list);
            }
            
            ArrayList<String> strlist = (ArrayList<String>) inner_key.get("genre");
            if(!strlist.contains(genre) && !genre.equals("")){
                strlist.add(genre);
                inner_key.replace("genre",strlist);
            }
        
        return inner_key;
    }
    
    
    private static Document computeInnerDirectorsDocument(Document inner_key, int directorid, String first, String last){
        /*if(inner_key == null){
            inner_key =  new Document();
            ArrayList<Integer> list = new ArrayList<>();
            if(!list.contains(directorid) && directorid != -1){
                list.add(directorid);
                inner_key.replace("directorid",list);
            }
            
            ArrayList<String> strlist = new ArrayList<>();
            if(!strlist.contains(first) && !first.equals("")){
                strlist.add(first);
                inner_key.replace("first",strlist);
            }
            
            strlist = new ArrayList<>();
            if(!strlist.contains(last) && !last.equals("")){
                strlist.add(last);
                inner_key.replace("last",strlist);
            }
            
        }
        else{
            
            ArrayList<Integer> list = (ArrayList<Integer>) inner_key.get("directorid");
            if(!list.contains(directorid) && directorid != -1){
                list.add(directorid);
                inner_key.replace("directorid",list);
            }
            
            ArrayList<String> strlist = (ArrayList<String>) inner_key.get("first");
            if(!strlist.contains(first) && !first.equals("")){
                strlist.add(first);
                inner_key.replace("first",strlist);
            }
            
            strlist = (ArrayList<String>) inner_key.get("last");
            if(!strlist.contains(last) && !last.equals("")){
                strlist.add(last);
                inner_key.replace("last",strlist);
            }
            
        
        return inner_key;
    }
    
    
    private static Document computeInnerActorsDocument(Document inner_key, int actorid, String afirst, String alast, String arole){
        /*if(inner_key == null){
            System.out.println("In actors check == null");
            inner_key =  new Document();
            
            ArrayList<Integer> list = new ArrayList<>();
            if(!list.contains(actorid) && actorid != -1){
                list.add(actorid);
                inner_key.replace("actorid",list);
            }
            
            ArrayList<String> strlist = new ArrayList<>();
            if(!strlist.contains(afirst) && !afirst.equals("")){
                strlist.add(afirst);
                inner_key.replace("afirst",strlist);
            }
            
            strlist = new ArrayList<>();
            if(!strlist.contains(alast) && !alast.equals("")){
                strlist.add(alast);
                inner_key.replace("alast",strlist);
            }
            
            strlist = new ArrayList<>();
            if(!strlist.contains(arole) && !arole.equals("")){
                strlist.add(arole);
                inner_key.replace("aroles",strlist);
            }
            
        }
        else{
            
            ArrayList<Integer> list = (ArrayList<Integer>) inner_key.get("actorid");
            if(!list.contains(actorid) && actorid != -1){
                list.add(actorid);
                inner_key.replace("actorid",list);
            }
            
            
            ArrayList<String> strlist = (ArrayList<String>) inner_key.get("afirst");
            if(!strlist.contains(afirst) && !afirst.equals("")){
                strlist.add(afirst);
                inner_key.replace("afirst",strlist);
            }
            
            
            strlist = (ArrayList<String>) inner_key.get("alast");
            if(!strlist.contains(alast) && !alast.equals("")){
                strlist.add(alast);
                inner_key.replace("alast",strlist);
            }
            
            
            strlist = (ArrayList<String>) inner_key.get("arole"); 
            if(!strlist.contains(arole) && !arole.equals("")){
                strlist.add(arole);
                inner_key.replace("arole",strlist);
            }
               
        return inner_key;
    }*/
    
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
    
    private static List<Document> createList(){
        
        List<Document> doc_list = new ArrayList<>(id_map.values());
        
        return doc_list;
    }
   
    
    private static boolean findDocument(ArrayList<Document> inner_list, int id, String type){
        for(Document doc : inner_list){
            if(id == doc.getInteger(type)){
                return true;
            }       
        }
        return false;
    }
    
    private static Document addActorEntry(int actorid, String afirst, String alast, String arole){
        Document doc = new Document();
        doc.put("actorid", actorid);
        doc.put("afirst",afirst);
        doc.put("alast", alast);
        doc.put("arole",arole);
        return doc;
    
    }
    
    private static Document addDirectorEntry(int directorid, String first, String last){
        Document doc = new Document();
        doc.put("directorid", directorid);
        doc.put("first",first);
        doc.put("last", last);
        return doc;
    }
    
    private static Document addGenreEntry(int genreid, String genre){
        Document doc = new Document();
        doc.put("genreid", genreid);
        doc.put("genre",genre);
        return doc;
    }
    
    
    private static void computeDocument(int movieid, int actorid, String afirst, String alast, String arole, int directorid, String first, String last, int genreid, String genre){
        Document current = id_map.get(movieid);
        
        //actors
        ArrayList<Document> inner_list = (ArrayList<Document>) current.get("actors");
        if(inner_list.size() > 0){
            if ( actorid != -1 && !findDocument(inner_list,actorid, "actorid") ){
                Document new_doc = addActorEntry(actorid, afirst, alast, arole);
                inner_list.add(new_doc);
            }
        }
        else if ( actorid!= -1 && inner_list.isEmpty() ){
            Document new_doc = addActorEntry(actorid, afirst, alast, arole);
            inner_list.add(new_doc);
        }
        current.replace("actors",inner_list);
        
        //directors
        inner_list = (ArrayList<Document>) current.get("directors");
        if(inner_list.size() > 0){
            if ( directorid != -1 && !findDocument(inner_list,directorid, "directorid")){
                Document new_doc = addDirectorEntry(directorid, first, last);
                inner_list.add(new_doc);
            }
        }
        else if (directorid != -1 && inner_list.isEmpty()){
            Document new_doc = addDirectorEntry(directorid, first, last);
            inner_list.add(new_doc);
        }
        current.replace("directors",inner_list);
        
        //genres
        inner_list = (ArrayList<Document>) current.get("genres");
        if(inner_list.size() > 0){
            if ( genreid != -1 && !findDocument(inner_list,genreid, "genreid")){
                Document new_doc = addGenreEntry(genreid, genre);
                inner_list.add(new_doc);
            }
        }
        else if (genreid != -1 && inner_list.isEmpty()){
            Document new_doc = addGenreEntry(genreid, genre);
            inner_list.add(new_doc);
        }
        current.replace("genres",inner_list);
        
        id_map.put(movieid, current);
        
        
        /*
        Document current = id_map.get(movieid);
        Document inner_key = (Document) current.get("actors");
        inner_key = computeInnerActorsDocument(inner_key,actorid,afirst,alast,arole); //return and replace in the outer document;
        current.replace("actors",inner_key);
        
        inner_key = (Document) current.get("directors");
        inner_key = computeInnerDirectorsDocument(inner_key,directorid,first,last); //return and replace in the outer document;
        current.replace("directors",inner_key);
    
        inner_key = (Document) current.get("genres");
        inner_key = computeInnerGenresDocument(inner_key,genreid,genre); //return and replace in the outer document;
        current.replace("genres",inner_key);
        
        id_map.put(movieid,current);*/
        
    }
    
    
    private static void updateDocuments() throws SQLException{
        String cs_str_values = convertToCommaSeparatedValues();
        
        String query = "Select movieid, actorid, afirst, alast, arole, directorid, first, last, genreid, genre From\n" +
                       "(select movieid, actorid, actors.first as afirst, actors.last as alast, role as arole, null as directorid, null as first, null as last, null as genreid, null as genre "+
                       "From roles join actors on actors.id = roles.actorid Where movieid in ( " + cs_str_values + " )" + "\n" +
                       "Union all\n" +
                       "Select movieid, null as actorid, null as first, null as last, null as arole, directorid, directors.first, directors.last, null as genreid, null as genre  "+
                       "from directedby join directors on directors.id = directedby.directorid Where movieid in ( " + cs_str_values + " )" + "\n" +
                       "union all\n" +
                       "select movieid, null as actorid ,null as afirst, null as alast, null as arole, null as directorid, null as first, null as last, genreid, genre "+
                       "from moviegenres join genres on genres.id = moviegenres.genreid Where movieid in ( " + cs_str_values + " )" + " ) temp;";
        
        Statement stmt_new = mysql_con.createStatement();
        ResultSet result_new = stmt_new.executeQuery(query);
        
        stmt_new.setFetchSize(20000);
        
        while(result_new.next()){
            
            int movieid = result_new.getInt(1);
            int actorid = result_new.getInt(2);
            if(result_new.wasNull()){
                actorid = -1;
            }
            String afirst = result_new.getString(3);
            if(result_new.wasNull()){
                afirst = "";
            }
            String alast = result_new.getString(4);
            if(result_new.wasNull()){
                alast = "";
            }
            String arole = result_new.getString(5);
            if(result_new.wasNull()){
                arole = "";
            }
            int directorid = result_new.getInt(6);
            if(result_new.wasNull()){
                directorid = -1;
            }
            String first = result_new.getString(7);
            if(result_new.wasNull()){
                first = "";
            }
            String last = result_new.getString(8);
            if(result_new.wasNull()){
                last = "";
            }
            int genreid = result_new.getInt(9);
            if(result_new.wasNull()){
                genreid = -1;
            }
            String genre = result_new.getString(10);
            if(result_new.wasNull()){
                genre = "";
            }
            
            computeDocument(movieid, actorid, afirst, alast, arole, directorid, first, last, genreid, genre);
            
        }
        
        collection.insertMany(createList());
        
        result_new.close();
        stmt_new.close();
     
    }
    
    private static void populate_Movies() throws SQLException{
        int ctr = 0;
        stmt = mysql_con.createStatement();
        collection = mongo_db.getCollection("movies");
        
        result = stmt.executeQuery("use Movies;");
        String query = "select id, title, year from movies;";
        result = stmt.executeQuery(query);
        stmt.setFetchSize(5000);
        
        while(result.next()){
            int movieid = result.getInt(1);
            String title = result.getString(2);
            int year = result.getInt(3);
            
            Document document = new Document();
            document.put("_id",movieid);
            document.put("title",title);
            document.put("year",year);
            document.put("actors",new ArrayList<Document>());
            document.put("directors",new ArrayList<Document>());
            document.put("genres",new ArrayList<Document>());
            
            //document.put("actors",new Document());
            //document.put("directors",new Document());
            //document.put("genres",new Document());
            //document.put("actors", null);
            //document.put("directors", null);
            //document.put("genres", null);
            
            /*Document inner_doc = new Document();
            inner_doc.put("actorid", new ArrayList<Integer>());
            inner_doc.put("afirst", new ArrayList<String>());
            inner_doc.put("alast", new ArrayList<String>());
            inner_doc.put("arole", new ArrayList<String>());
            document.put("actors",inner_doc);
            
            inner_doc = new Document();
            inner_doc.put("directorid", new ArrayList<Integer>());
            inner_doc.put("first", new ArrayList<String>());
            inner_doc.put("last", new ArrayList<String>());
            document.put("directors",inner_doc);
            
            inner_doc = new Document();
            inner_doc.put("genreid", new ArrayList<Integer>());
            inner_doc.put("genre", new ArrayList<String>());
            document.put("genres",inner_doc);
            */
            
            
            if(!id_map.containsKey(movieid)){
                id_map.put(movieid,document);
                ctr ++;
            }
            
            if (ctr == 5000){
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
            System.out.println("All records added to Movies");
        
        }
    
    }
    
    
    
    private static void createCollection(){ 
        mongo_db = mongo.getDatabase("imdb_denormalized");
        MongoCollection<Document> collection = mongo_db.getCollection("movies");
        collection.drop(); 
        mongo_db.createCollection("movies");
        System.out.println("Created Collection Movies");
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
    
    public static void main(String args[]) throws ClassNotFoundException, SQLException{
        
        mongoConnection();
        mySQLConnection();
        createCollection();
        System.out.println("Loading into Movies...");
        populate_Movies();
    }
}

