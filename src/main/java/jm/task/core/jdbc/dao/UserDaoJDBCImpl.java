package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String sql = "CREATE TABLE USERS " +
                "(id INTEGER not NULL AUTO_INCREMENT, " +
                " name VARCHAR(255), " +
                " last_name VARCHAR(255), " +
                " age INTEGER, " +
                " PRIMARY KEY ( id ))";
        try (Connection connection = Util.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("PP_1_1_3-4_JDBC_Hibernate", null, "USERS",
                    new String[]{"TABLE"});
            if (!tables.first()) {
                System.out.println("\nCreate new table");
                statement.executeUpdate();
            } else {
                System.out.println("\nTable already exists");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
    public void dropUsersTable() {
        String sql = "DROP TABLE USERS ";
        try (Connection connection = Util.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet tables = metaData.getTables("PP_1_1_3-4_JDBC_Hibernate"
                    , null
                    , "USERS"
                    , new String[]{"TABLE"});
            if (tables.first()) {
                statement.executeUpdate();
                System.out.println("\nTable USERS is dropped");
            } else {
                System.out.println("\nNo such table exists in the database");
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO USERS (name, last_name, age)" +
                "VALUES ('" + name + "', '" + lastName + "', " + (int) age + ")";
        try (Connection connection = Util.getConnection(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
            System.out.println("User " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public void removeUserById(long id) {
        String sql = "DELETE FROM USERS WHERE id = " + id;
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
            System.out.println("\nUser deleted from table by id - " + id);
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    public List<User> getAllUsers() {
        List<User> userList = new ArrayList<>();
        int i = 0;
        String sql = "SELECT id, name, last_name, age FROM USERS";
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                userList.add(new User(resultSet.getString("name")
                        , resultSet.getString("last_name")
                        , (byte) resultSet.getInt("age")));
                userList.get(i++).setId((long) resultSet.getInt("id"));
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return userList;
    }

    public void cleanUsersTable() {
        String sql = "DELETE FROM USERS";
        try (Connection connection = Util.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.executeUpdate();
            System.out.println("\nThe table cleaned");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
