

import java.io.File;
import java.util.*;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;


public class Neo4j_Migration {
    static Connection mysql_con = null;
    static Statement stmt = null;
    static Statement neo4j_stmt = null;
    static String database_url = "jdbc:mysql://localhost:3306/Movies";
    static String sql_username = "root";
    static String  sql_password = "root";
    static ResultSet result = null;
    static PreparedStatement prepstmt = null;   
    static Connection neo4j_con = null;
    static HashMap<Integer,Long> movies_id_map = new HashMap<>();
    static HashMap<Integer,Long> actors_id_map = new HashMap<>();
    static HashMap<Integer,Long> directors_id_map = new HashMap<>();
    static HashMap<Integer,Long> genre_id_map = new HashMap<>();
    static String db_path = "C:\\Users\\Shweta Yakkali\\Documents\\Neo4j\\default.graphdb\\Movies";
    static String db_name = "use Movies;";
    static BatchInserter inserter = null;
    
    /*private static void neo4jConnection() throws SQLException{
        neo4j_con = DriverManager.getConnection("jdbc:neo4j:bolt://localhost:7687",neo4j_username,neo4j_password);
        System.out.println("Connected to Neo4j Server");
    
    }*/
    
    private static void mySQLConnection() throws ClassNotFoundException, SQLException{
        Class.forName("com.mysql.jdbc.Driver");  
        mysql_con = DriverManager.getConnection(database_url,sql_username,sql_password);
        System.out.println("Connected to MySQL Server");
    }
    
    
    private static void create_MovieGenresRelation() throws SQLException, IOException{
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select mg.genreid, mg.movieid from moviegenres mg;";
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            int genreid = result.getInt(1);
            int movieid = result.getInt(2);
            
            long genre_uuid = genre_id_map.get(genreid);
            long movie_uuid = movies_id_map.get(movieid);
            
            inserter.createRelationship(movie_uuid, genre_uuid, RelationshipType.withName("Genre_Type"), null);                   
        }
        //inserter.shutdown();
        System.out.println("Genre_Type relation with MovieGenres property created");        
        result.close();
        stmt.close();
        
    }
    
    
    private static void create_DirectedByRelation() throws SQLException, IOException{
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select db.directorid, db.movieid from directedby db;";
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            int directorid = result.getInt(1);
            int movieid = result.getInt(2);
            
            long director_uuid = directors_id_map.get(directorid);
            long movie_uuid = movies_id_map.get(movieid);
            
            inserter.createRelationship(movie_uuid, director_uuid, RelationshipType.withName("Directed_By"), null);                   
        }
        //inserter.shutdown();
        System.out.println("Directed_By relation with DirectedBy property created");
        result.close();
        stmt.close();
    }
    
    
    
    private static void create_RolesRelation() throws SQLException, IOException{
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select r.actorid, r.movieid, r.role from roles r;";
        stmt.setFetchSize(25000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            
            int actorid = result.getInt(1);
            int movieid = result.getInt(2);
            String role = result.getString(3);
            long actor_uuid = actors_id_map.get(actorid);
            long movie_uuid = movies_id_map.get(movieid);
            
            if(!result.wasNull()){
                Map<String,Object> role_property = new HashMap<>();
                role_property.put("role",role);                            
                inserter.createRelationship(movie_uuid, actor_uuid, RelationshipType.withName("Acted_In"), role_property); 
            
            }
            
            else {                         
                inserter.createRelationship(movie_uuid, actor_uuid, RelationshipType.withName("Acted_In"), null);
            }          
                              
        }
        //inserter.shutdown();
        System.out.println("Acted_In relation with role property created");
        result.close();
        stmt.close();
    }
    
    
    
    private static void create_MovieNodes() throws SQLException, IOException{
        
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select id, title, year from movies;";
                    
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            int id = result.getInt(1);
            String title = result.getString(2);
            String year = result.getString(3);
            
            Map<String,Object> map_node = new HashMap<>();
            map_node.put("movieid",id);
            map_node.put("title",title);
            map_node.put("year",year);
            
            long movie_node = inserter.createNode(map_node, Label.label("Movies"));
            if(!movies_id_map.containsKey(id)){
                movies_id_map.put(id,movie_node);
            }
        }
        
        //inserter.shutdown();
        System.out.println("Movie Nodes created");
        result.close();
        stmt.close();
    
    }
    
    
    
    private static void create_DirectorNodes() throws SQLException, IOException{
        
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select id, first, last from directors;";
                    
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            int id = result.getInt(1);
            String first = result.getString(2);
            String last = result.getString(3);
            
            Map<String,Object> map_node = new HashMap<>();
            map_node.put("directorid",id);
            map_node.put("first",first);
            map_node.put("last",last);
            
            long director_node = inserter.createNode(map_node, Label.label("Directors"));
            if(!directors_id_map.containsKey(id)){
                directors_id_map.put(id,director_node);
            }
        }
        
        //inserter.shutdown();
        System.out.println("Director Nodes created");       
        result.close();
        stmt.close();
    
    }
    
    
    private static void create_ActorNodes() throws SQLException, IOException{
        
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select id, first, last, gender from actors;";
                    
        stmt.setFetchSize(10000);
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
        
        while(result.next()){
            
            int id = result.getInt(1);
            String first = result.getString(2);
            String last = result.getString(3);
            String gender = result.getString(4);
            
            Map<String,Object> map_node = new HashMap<>();
            map_node.put("actorid",id);
            map_node.put("first",first);
            map_node.put("last",last);
            map_node.put("gender",gender);
            
            long actor_node = inserter.createNode(map_node, Label.label("Actors"));
            if(!actors_id_map.containsKey(id)){
                actors_id_map.put(id,actor_node);
            }
        }
        
        //inserter.shutdown();
        System.out.println("Actor Nodes created");        
        result.close();
        stmt.close();
    
    }
    
    private static void create_GenreNodes() throws SQLException, IOException{
        int ctr = 0;
        
        stmt = mysql_con.createStatement();
        result = stmt.executeQuery(db_name);
        String query = "select id, genre from genres;";
        
        result = stmt.executeQuery(query);
        
        //inserter = BatchInserters.inserter(new File(db_path));
           
        while(result.next()){
            int id = result.getInt(1);
            String genre = result.getString(2);
            
            Map<String,Object> map_node = new HashMap<>();
            map_node.put("genreid",id);
            map_node.put("genre",genre);
            
            long genre_node = inserter.createNode(map_node, Label.label("Genres"));
            if(!genre_id_map.containsKey(id)){
                genre_id_map.put(id,genre_node);
            }
            
        }
        //inserter.shutdown();
        System.out.println("Genre Nodes created");
        result.close();
        stmt.close();
                
    }
    
    
    public static void main(String args[]) throws SQLException, ClassNotFoundException, IOException{
        
        //neo4jConnection();
        mySQLConnection();
        
        inserter = BatchInserters.inserter(new File(db_path));
        
        create_MovieNodes();
        create_ActorNodes();
        create_RolesRelation();
        actors_id_map.clear();
        
        create_DirectorNodes();        
        create_DirectedByRelation();
        directors_id_map.clear();
        
        create_GenreNodes();        
        create_MovieGenresRelation();
        genre_id_map.clear();
        
        inserter.shutdown();
        
               
    }
}
