
import java.sql.*;
import java.lang.*;
import java.util.HashMap;
import java.util.HashSet;

public class DataMigration_MySQL_old_to_new {
    static Connection conold = null;
    static Connection connew = null;
    static Connection conlocal = null;
    static Statement stmt = null;
    static String database_url = "jdbc:mysql://localhost:3306/Movies";
    static String database_url_new = "jdbc:mysql://localhost:3306/NewMoviesSchema";
    static String database_ref = "jdbc:mysql://localhost:3306";
    static String username = "root";
    static String  password = "root";
    static ResultSet result = null;
    static String query = "";
    static PreparedStatement prepstmt = null;
    //static PreparedStatement prep_actor_stmt = null;
    //static PreparedStatement prep_director_stmt = null;
    
    static HashSet<Integer> common_ids = new HashSet<>();
    static HashMap<Integer, Integer> old_new_map = new HashMap<>();
    
    static int person_id_ctr = 1;
    
    
    public static void insertIntoDirectorCommon() throws SQLException{
        
        
        try{
            conold.setAutoCommit(false);
            connew.setAutoCommit(false);
            int ctr = 0;
            stmt = conold.createStatement();
            query = "select db.directorid, db.movieid from Movies.directedby db, Movies.actors a where a.id = db.directorid;";
            
            stmt.setFetchSize(7000);
            result = stmt.executeQuery(query);


            String insertTableSQL = "insert into Director"
                                    + "(person_id, movie_id) values"
                                    + "(?,?)";
            prepstmt = connew.prepareStatement(insertTableSQL);


            while(result.next()){
                int director_old_id = result.getInt(1);
                int movie_id = result.getInt(2);
                if(old_new_map.containsKey(director_old_id)){
                    prepstmt.setInt(1, old_new_map.get(director_old_id));
                    prepstmt.setInt(2, movie_id);
                    ctr ++;
                    prepstmt.addBatch();
                }

                if(ctr % 7000 == 0){

                    prepstmt.executeBatch();
                    System.out.println("Rows Inserted :"+ctr);
                    ctr = 0;
                }

            }

            if( ctr > 0){                
                        prepstmt.executeBatch();
                    }

            connew.commit();
            System.out.println("Data populated in Director with new ids and respective movieids");
        }
        catch (Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            connew.rollback();
        }  
    
    }
    
    
    
    public static void insertIntoDirectorFromDirectdby() throws SQLException{
        try{
            //stmt = conold.createStatement();
            stmt = conlocal.createStatement();
            query = "INSERT HIGH_PRIORITY INTO NewMoviesSchema.Director(person_id,name) SELECT Movies.directedby.directorid, Movies.directedby.movieid " +
                    "FROM Movies.directedby " +
                    "WHERE Movies.directedby.directorid NOT IN (SELECT Movies.actors.id FROM Movies.actors, Movies.directors WHERE Movies.actors.id = Movies.directors.id);";
            stmt.executeUpdate(query);
        }
        catch(Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            
        }
        System.out.println("Data populated in Director from directedby (uncommon data)");
    
    
    
    }
    
    public static void insertIntoPersonCommon()throws SQLException{
        try{
            int ctr = 0;
            connew.setAutoCommit(false);
            conold.setAutoCommit(false);
            
            stmt = connew.createStatement();
            query = "select max(person_id) from Person;";
            result = stmt.executeQuery(query);
            int max_person_id = -1;
            while(result.next()){
                max_person_id = result.getInt(1); 

            }
            person_id_ctr = max_person_id;


            stmt = conold.createStatement();
            query = "select d.id, d.first, d.last from Movies.actors a, Movies.directors d where a.id = d.id;";
            stmt.setFetchSize(7000);
            result = stmt.executeQuery(query);

            String insertTableSQL = "insert into Person"
                                + "(person_id, name) values"
                                + "(?,?)";
            prepstmt = connew.prepareStatement(insertTableSQL);
            
            while(result.next()){
                int director_id_old = result.getInt(1);
                String name = result.getString(2) + result.getString(3);
                if(!old_new_map.containsKey(director_id_old)){
                    old_new_map.put(director_id_old, ++person_id_ctr );

                }
                prepstmt.setInt(1, old_new_map.get(director_id_old));
                prepstmt.setString(2, name);
                ctr++;
                prepstmt.addBatch();

                if(ctr % 7000 == 0){

                        prepstmt.executeBatch();
                        System.out.println("Rows Inserted :"+ctr);
                        ctr = 0;
                }

            }
            if( ctr > 0){                
                    prepstmt.executeBatch();
                }

            connew.commit();
            System.out.println("Data populated in Person - common data with new person ids for the common data");
        }
        catch (Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            connew.rollback();
        }
            
    }
    
    
    /*public static void computeCommon() throws SQLException{
        try{
            int ctr = 0;
            conold.setAutoCommit(false);
            connew.setAutoCommit(false);
            
            stmt = conold.createStatement();
            query = "select a.id from Movies.actors a, Movies.directors d where a.id = d.id;";
            
            stmt.setFetchSize(25000);
            result = stmt.executeQuery(query);
            while(result.next()){
                common_ids.add(result.getInt(1));
                
            }
            System.out.print("Common ids in actors and directors "+common_ids.size());
        }
        
        catch (Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            connew.rollback();
        }
    
    }
    */
    
    
    
    public static void insertIntoPersonFromDirectors() throws SQLException{
        try{
            //stmt = conold.createStatement();
            stmt = conlocal.createStatement();
            query = "INSERT HIGH_PRIORITY INTO NewMoviesSchema.Person(person_id,name) SELECT Movies.directors.id, " +
                    "concat(Movies.directors.first,' ',Movies.directors.last) FROM Movies.directors " +
                    "WHERE Movies.directors.id NOT IN ( SELECT Movies.actors.id FROM Movies.actors, Movies.directors WHERE Movies.actors.id = Movies.directors.id)";
            stmt.executeUpdate(query);
        }
        catch(Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            
        }
    
        System.out.println("Data populated in Person from director - uncommon ");
    
    }
    
    
    
    
    
    public static void insertIntoActorFromRoles(){
        try{
            //stmt = conold.createStatement();
            stmt = conlocal.createStatement();
            query = "insert HIGH_PRIORITY into NewMoviesSchema.Actor(person_id, movie_id) select actorid, movieid from Movies.roles";
            stmt.executeUpdate(query);
        }
        catch(Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            
        }
        System.out.println("Data populated in Actor from roles");
    }
    
    public static void insertIntoPersonFromActors() throws SQLException{
        try{
            //stmt = conold.createStatement();
            stmt = conlocal.createStatement();
            query = "insert HIGH_PRIORITY into NewMoviesSchema.Person(person_id, name) select id, concat(first,' ', last) from Movies.actors";
            stmt.executeUpdate(query);
        }
        catch(Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            
        }
        System.out.println("Data populated in Person from actors");
    }
    
    public static void insertIntoNewMovie() throws SQLException{
        try{
            //stmt = conold.createStatement();
            stmt = conlocal.createStatement();
            query = "insert HIGH_PRIORITY into NewMoviesSchema.Movie(movie_id, title, release_year) select id, title, year from Movies.movies";
            stmt.executeUpdate(query);
        }
        catch(Exception e){
            System.out.println("Exception Occurred");
            e.printStackTrace();
            
        }
        System.out.println("Data populated in Movie table");
    }
    
    
    
    public static void createTables() throws SQLException{
        
        //create Person
        query = "drop table if exists Person;";
        stmt.executeUpdate(query);
        query = "create table Person (person_id int(11) not null, name varchar(250), primary key(person_id));";
        stmt.executeUpdate(query);
        
        //create Movie
        query = "drop table if exists Movie;";
        stmt.executeUpdate(query);
        query = "create table Movie (movie_id int(11) not null, title varchar(250), release_year int(11), primary key(movie_id));";
        stmt.executeUpdate(query);
        
        
        //create Actor
        query = "drop table if exists Actor;";
        stmt.executeUpdate(query);
        query = "create table Actor (person_id int(11) not null, movie_id int(11) not null, primary key(person_id,movie_id))";//,\n"+
                //"foreign key(person_id) references Person(person_id) on delete cascade, foreign key(movie_id) references Movie(movie_id) on delete cascade);";
        stmt.executeUpdate(query);
        
        
        //create Director
        query = "drop table if exists Director;";
        stmt.executeUpdate(query);
        query = "create table Director (person_id int(11) not null, movie_id int(11) not null, primary key(person_id,movie_id));";//,\n"+
                //"foreign key(person_id) references Person(person_id) on delete cascade, foreign key(movie_id) references Movie(movie_id) on delete cascade);";
        stmt.executeUpdate(query);
            
    }
    
    public static void alterTableAddForeignKeys() throws SQLException{
        
        //add foreign key contrainsts to Actor Table
        stmt = connew.createStatement();
        query = "ALTER TABLE Actor ADD FOREIGN KEY (person_id) REFERENCES Person(person_id);";
        stmt.executeUpdate(query);
                
        query = "ALTER TABLE Actor ADD FOREIGN KEY (movie_id) REFERENCES Movie(movie_id);";
        stmt.executeUpdate(query);
        
        
        //add foreign key contraints to Director Table
        
        stmt = connew.createStatement();
        query = "ALTER TABLE Director ADD FOREIGN KEY (person_id) REFERENCES Person(person_id);";
        stmt.executeUpdate(query);
                
        query = "ALTER TABLE Director ADD FOREIGN KEY (movie_id) REFERENCES Movie(movie_id);";
        stmt.executeUpdate(query);
    
    }
    
    public static void main(String args[]){
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conold = DriverManager.getConnection(database_url,username,password);
            connew = DriverManager.getConnection(database_url_new,username,password);
            conlocal = DriverManager.getConnection(database_ref,username,password);
            
            
            stmt = connew.createStatement();
            query = "drop database if exists NewMoviesSchema";
            stmt.executeUpdate(query);
            
            query = "create database NewMoviesSchema;";
            stmt.executeUpdate(query);
            
            result = stmt.executeQuery("use NewMoviesSchema;");
            
            createTables();
            
            insertIntoNewMovie();
            
            insertIntoPersonFromActors();
            
            insertIntoActorFromRoles();
            
            insertIntoPersonFromDirectors();
            
            insertIntoPersonCommon();
            
            insertIntoDirectorFromDirectdby();
            
            insertIntoDirectorCommon();
            
            alterTableAddForeignKeys();
            
            conold.setAutoCommit(true);
            connew.setAutoCommit(true);
            
            conold.close();
            connew.close();
        }
        catch(Exception e){
            System.out.println(e);
        }
    }
}
