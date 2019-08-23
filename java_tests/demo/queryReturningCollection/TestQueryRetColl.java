package java_tests.demo.queryReturningCollection;

import java.sql.*;

public class TestQueryRetColl {

    public final static String DB_URL = "jdbc:oracle:thin:@localhost:151/orcl";
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
        trace_on(conn);
        do_test(conn);
        trace_off(conn);
        drop_objects(conn);

        System.out.println("INFO: Main completed");
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
        System.out.println("INFO: Running query ... ");
        query = "SELECT * FROM emp WHERE rownum<=10";
        System.out.println("QUERY: " + query);
        rs = st.executeQuery(query);
        while (rs.next()){
            System.out.println("ID: " + rs.getInt(1)  + ", " + rs.getString(2));
        }
        rs.close();
        st.close();
    }

}
