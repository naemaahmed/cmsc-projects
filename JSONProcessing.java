import java.sql.*;
import java.util.Scanner;
import org.json.simple.*;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONValue;


public class JSONProcessing 
{
    private static Connection connection;
	public static void processJSON(String json) {

        JSONObject object = (JSONObject) JSONValue.parse(json);
        String query = "";
        Boolean duplicate = false;
        Boolean flag = false;
        Statement stmt = null;
        ResultSet rs = null;

        JSONObject newcustomer = (JSONObject) object.get("newcustomer");

        if(newcustomer != null){
            String check = "SELECT name * from customers where customerid = '" + newcustomer.get("customerid") + "';";
            try {
                stmt = connection.createStatement();
                rs = stmt.executeQuery(check);
                stmt.close();
            }catch (SQLException e){
                System.out.println(e);
            }

                //no previours record so insert customer info
                //check match for frequentflieron
                try {
                        stmt = connection.createStatement();
                        query = ("(SELECT airlineid from airlines where name like '%"  + newcustomer.get("frequentflieron") + "%');");
                        if(stmt.executeQuery(query).isBeforeFirst()){
                            flag = true;
                        }else{
                            flag = false;
                        }
                        stmt.close();
                    }catch (SQLException e){
                        System.out.println(e);
                    }
                if(flag){
                    query = "INSERT INTO customers VALUES ('" + newcustomer.get("customerid") + "', '" + newcustomer.get("name") +  "', '" + newcustomer.get("birthdate") + "', " + "(SELECT airlineid from airlines where name like '%"  + newcustomer.get("frequentflieron") + "%'));";
                    System.out.println(query);
                    try {
                        stmt = connection.createStatement();
                        rs = stmt.executeQuery(query);
                        stmt.close();
                    }catch (SQLException e ) {
                        System.out.println(e);
                    }
                }else{
                    System.out.println("The airline does not exist.");
                }
        }else{
            //insert flightinfo
            JSONObject flightinfo = (JSONObject) object.get("flightinfo");

            // loop array
            JSONArray c = (JSONArray) flightinfo.get("customers");
            JSONObject customer;
            Iterator<JSONObject> iterator = c.iterator();

            while (iterator.hasNext()) {
                customer = iterator.next();
                System.out.println(customer.get("customer_id"));
                if (customer.get("name") != null){
                    try {
                        stmt = connection.createStatement();
                        stmt.close();
                    }catch (SQLException e){
                        System.out.println(e);
                    }

                        query = "INSERT INTO customers VALUES ('" + customer.get("customer_id") + "', '" + customer.get("name") 
                            +  "', '" + customer.get("birthdate") + "', '"+ customer.get("frequentflieron") + "');";
                        System.out.println(query); 

                        try {
                            stmt = connection.createStatement();
                            rs = stmt.executeQuery(query);
                            stmt.close();
                        }catch (SQLException e){

                            System.out.println(e);
                        }
                        query = "INSERT INTO flewon VALUES ('" + flightinfo.get("flightid") + "', '" + customer.get("customer_id") + "', '" + flightinfo.get("flightdate") + "');";
                        System.out.println(query); 
                        try {
                            stmt = connection.createStatement();
                            rs = stmt.executeQuery(query);
                            stmt.close();
                        } catch (SQLException e ) {
                            System.out.println(e);
                        }
          
                }else { 
                    //if not add another entry to the flew on table
                    query = "INSERT INTO flewon VALUES ('" + flightinfo.get("flightid") + "', '" + customer.get("customer_id") + "', '" + flightinfo.get("flightdate") + "');";
                    try {
                        stmt = connection.createStatement();
                        rs = stmt.executeQuery(query);
                        stmt.close();
                    } catch (SQLException e ) {
                        System.out.println(e);
                    }
                    System.out.println(query);  
                } 
            }
        }
	}

	public static void main(String[] argv) {
		System.out.println("-------- PostgreSQL " + "JDBC Connection Testing ------------");
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
            e.printStackTrace();
            return;
        }
        System.out.println("PostgreSQL JDBC Driver Registered!");
        try {
            connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/flightsskewed","vagrant", "vagrant");
        } catch (SQLException e) {
            System.out.println("Connection Failed! Check output console");
            e.printStackTrace();
            return;
        }

        if (connection != null) {
            System.out.println("You made it, take control your database now!");
        } else {
            System.out.println("Failed to make connection!");
            return;
        }
        Scanner in_scanner = new Scanner(System.in);
		while(in_scanner.hasNext()) {
			String json = in_scanner.nextLine();
			processJSON(json);
		}
	}
}
