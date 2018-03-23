/*
create table tst (id number, txt varchar2(10))
/
*/

import java.sql.*;
import oracle.jdbc.OracleConnection;
import oracle.sql.ArrayDescriptor;
import oracle.sql.ARRAY;

public class JdbcBatchInsert {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int obj_id = 0;
	  int count = 0;

    Connection conn = null;
    DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
    conn = DriverManager.getConnection("jdbc:oracle:thin:@localhost:151/orcl","devdata","dev");
    conn.setAutoCommit(false);

    // Debug
    PreparedStatement ps = conn.prepareStatement(
         "INSERT INTO tst VALUES (?,?)");

    for (int i=1;i<10;i++){
       ps.setInt(1,i);
       ps.setString(2,"XXXX");
       ps.addBatch();
    }
    ps.executeBatch();
		conn.commit();
    ps.close();

	}

}
