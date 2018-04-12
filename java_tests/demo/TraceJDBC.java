package demo;

import java.util.ArrayList;

import java.sql.*;
import oracle.jdbc.OracleConnection;
import oracle.jdbc.OracleTypes;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;

public class TraceJDBC {

    public final static String DB_URL = "jdbc:oracle:thin:@localhost:1531/orcl";
    public final static String DB_USERNAME = "devdata";
    public final static String DB_PASSWORD = "dev";
    public final static String TST_TABLE = "TST_FETCH";

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
        trace_on(conn);
        do_test(conn);
        trace_off(conn);
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
        ps = conn.prepareStatement("SELECT count(*) FROM user_tables WHERE table_name = ?");
        ps.setString(1,TST_TABLE);
        rs = ps.executeQuery();
        while (rs.next()) cnt = rs.getInt(1);

        if (cnt==0) {
            System.out.println("INFO: Creating objects ...");
            query = "CREATE TABLE " + TST_TABLE + " as SELECT * FROM dba_objects where rownum<=1000";
            st.executeQuery(query);
            System.out.println("SUCCESS: TABLE " + TST_TABLE + " created.");
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
        ps.setString(1,TST_TABLE);
        rs = ps.executeQuery();
        while (rs.next()) cnt = rs.getInt(1);

        if (cnt>0) {
            System.out.println("INFO: Dropping objects ...");

            query = "DROP TABLE " + TST_TABLE + " PURGE";
            st.executeQuery(query);
            System.out.println("SUCCESS: TABLE " + TST_TABLE + " dropped.");
        }

        rs.close();
        ps.close();
        st.close();
    }

    static void trace_on(Connection conn) throws Exception {
        Statement st = conn.createStatement();
        String query;
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Turning on trace ...");
        query = "alter session set tracefile_identifier='TST_FETCH'";
        st.executeQuery(query);
        query = "alter session set events '10046 trace name context forever, level 12'";
        st.executeQuery(query);
        st.close();
    }

    static void trace_off(Connection conn) throws Exception {
        Statement st = conn.createStatement();
        String query;
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Turning off trace ...");
        query = "alter session set events '10046 trace name context off'";
        st.executeQuery(query);
        st.close();
    }

    static void do_test(Connection conn) throws Exception {
        Statement st = conn.createStatement();
        ResultSet rs = null;
        String query;
        System.out.println("-------------------------------------------------------------");
        System.out.println("INFO: Running query ... (always hard parsed)");
        query = "SELECT /* " + System.currentTimeMillis() + "*/ * FROM " + TST_TABLE + " WHERE rownum<=10";
        System.out.println("QUERY: " + query);
        rs = st.executeQuery(query);
        while (rs.next()){
            int i = 0;
        }
        rs.close();
        st.close();
    }

}
