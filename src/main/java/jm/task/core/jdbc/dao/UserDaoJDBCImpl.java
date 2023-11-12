package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.*;

public class UserDaoJDBCImpl implements UserDao {

    private static final Connection connection = Util.getConnection();

    private final static String CREATE_TABLE = new StringBuilder().append("CREATE TABLE IF NOT EXISTS `users` (\n")
            .append("  `id` INT NOT NULL AUTO_INCREMENT,\n")
            .append("  `name` VARCHAR(45) NULL,\n")
            .append("  `lastName` VARCHAR(45) NULL,\n")
            .append("  `age` INT NULL,\n")
            .append("        PRIMARY KEY (`id`))").toString();
    private final static String DROP = "DROP TABLE IF EXISTS `users`";
    private final static String ADD = "INSERT INTO `users` (name, lastName, age) values(?,?,?)";
    private final static String REMOVE_USER = "DELETE FROM `users` WHERE `id` = ?";
    private final static String SELECT = "SELECT * FROM `users`";
    private final static String DELETE ="TRUNCATE TABLE `users`";

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try(PreparedStatement  stm = connection.prepareStatement(CREATE_TABLE)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void dropUsersTable() {
        try(PreparedStatement  stm = connection.prepareStatement(DROP)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        try(PreparedStatement  stm = connection.prepareStatement(ADD)) {
            connection.setAutoCommit(false);
            stm.setString(1, name);
            stm.setString(2, lastName);
            stm.setByte(3, age);
            stm.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public void removeUserById(long id) {
        try(PreparedStatement  stm = connection.prepareStatement(REMOVE_USER)) {
            connection.setAutoCommit(false);
            stm.setLong(1, id);
            stm.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        try(PreparedStatement stm = connection.prepareStatement(SELECT)) {
            ResultSet resultSet = stm.executeQuery();
            while(resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                String lastName = resultSet.getString("lastName");
                byte age = resultSet.getByte("age");
                System.out.println("--------------------------------");
                System.out.println("User ID:" + id);
                System.out.println("User name:" + name);
                System.out.println("User lastName:" + lastName);
                System.out.println("User age:" + age);
                users.add(new User(name, lastName, age));
            }
        } catch (SQLException e) {
            return null;
        }
        return users;
    }

    public void cleanUsersTable() {
        try (PreparedStatement stm = connection.prepareStatement(DELETE)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
    }
}
