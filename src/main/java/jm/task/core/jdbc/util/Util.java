package jm.task.core.jdbc.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private static Connection connection;


    public static Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
//            System.out.println("\tUtil. Start opening connection");
            String url = "jdbc:mysql://localhost/PP_1_1_3-4_JDBC_Hibernate";
            String user = "root";
            String pass = "12345678";
            connection = DriverManager.getConnection(url, user, pass);
        } else {
//            System.out.println("\tUtil. Connection is opened");
        }
        return connection;
    }

    public static void closeConnection() throws SQLException {
        connection.close();
    }
}
