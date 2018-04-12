package demo;

import java.util.ArrayList;

import java.sql.*;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

public class JdbcCallProcPassingCollection {

    public final static String DB_URL = "jdbc:oracle:thin:@localhost:1531/orcl";
    public final static String DB_USERNAME = "devdata";
    public final static String DB_PASSWORD = "dev";

    public static Connection getConnection() throws ClassNotFoundException, SQLException {
        Connection conn = null;
        DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
        conn = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
        conn.setAutoCommit(false);
        System.out.println("DB Connection created successfully");
        return conn;
    }

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int obj_id = 0;
	    int count = 0;

        Connection conn = getConnection();

        create_objects(conn);

        do_test(conn);

        drop_objects(conn);

        System.out.println("INFO: Main completed");
	}

	static void create_objects(Connection conn) throws Exception {
	    PreparedStatement ps = null;
	    ResultSet rs = null;
	    Statement st = conn.createStatement();
	    String query;
        int cnt = -1;
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Checking if objects exists before creating ...");
        ps = conn.prepareStatement("SELECT count(*) FROM user_types WHERE type_name = ?");
	    ps.setString(1,"FOOD_T");
	    rs = ps.executeQuery();
        while (rs.next()) cnt = rs.getInt(1);

        if (cnt==0) {
            System.out.println("INFO: Creating objects ...");
            query = "CREATE TYPE food_t AS OBJECT (\n" +
                    "   name VARCHAR2 ( 100 )\n" +
                    " , food_group VARCHAR2 ( 100 )\n" +
                    " , color VARCHAR2 ( 100 )\n" +
                    ")";
            st.executeQuery(query);
            System.out.println("SUCCESS: TYPE food_t created.");

            query = "CREATE TYPE meals_at IS TABLE OF food_t";
            st.executeQuery(query);
            System.out.println("SUCCESS: TYPE meals_at created.");

            query = "CREATE TABLE food (\n" +
                    "   id   NUMBER\n" +
                    " , name VARCHAR2(100 CHAR)\n" +
                    " , food_group VARCHAR2 ( 100 CHAR )\n" +
                    " , color VARCHAR2 ( 100 CHAR)\n" +
                    ")";
            st.executeQuery(query);
            System.out.println("SUCCESS: TABLE food created.");

            query = "CREATE OR REPLACE PROCEDURE insert_food(meals_i IN meals_at) AS \n" +
                    "BEGIN\n" +
                    "   INSERT INTO food " +
                    "      SELECT rownum, name, food_group, color" +
                    "      FROM table(meals_i);" +
                    "END;";
            st.executeQuery(query);
            System.out.println("SUCCESS: PROCEDURE insert_food created.");

        }

        rs.close();
        ps.close();
        st.close();
    }

    static void drop_objects(Connection conn) throws Exception {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Statement st = conn.createStatement();
        String query;
        int cnt = -1;
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Checking if objects exists before dropping ...");
        ps = conn.prepareStatement("SELECT count(*) FROM user_types WHERE type_name = ?");
        ps.setString(1,"FOOD_T");
        rs = ps.executeQuery();
        while (rs.next()) cnt = rs.getInt(1);

        if (cnt>0) {
            System.out.println("INFO: Dropping objects ...");

            query = "DROP PROCEDURE insert_food";
            st.executeQuery(query);
            System.out.println("SUCCESS: PROCEDURE insert_food dropped.");

            query = "DROP TABLE food PURGE";
            st.executeQuery(query);
            System.out.println("SUCCESS: TABLE food dropped.");

            query = "DROP TYPE meals_at FORCE";
            st.executeQuery(query);
            System.out.println("SUCCESS: TYPE meals_at dropped.");

            query = "DROP TYPE food_t FORCE";
            st.executeQuery(query);
            System.out.println("SUCCESS: TYPE food_t dropped.");

        }

        rs.close();
        ps.close();
        st.close();
    }

    static void do_test(Connection conn) throws Exception {
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Looking up object definitions ...");
//        ArrayList<STRUCT> arow = new ArrayList<STRUCT>();
//        StructDescriptor voRowStruct = StructDescriptor.createDescriptor("FOOD_T", conn);
//        ArrayDescriptor arraydesc = ArrayDescriptor.createDescriptor("MEALS_AT",  conn);
//        OracleCallableStatement cs = null;
//
//        ARRAY p_meals_list = null;
//
//
//        cs = conn.prepareCall("{call PKG_INCOMPATIBILITY_CHECK.PROC_CHECK_INCOMPATIBILITY(:1)}");
//        cs.setArray(1, this.p_message_list);
//        cs.execute();
//        cs.close();
    }

}
