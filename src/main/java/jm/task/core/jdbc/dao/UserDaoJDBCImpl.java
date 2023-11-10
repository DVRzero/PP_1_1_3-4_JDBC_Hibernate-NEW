package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.*;

public class UserDaoJDBCImpl implements UserDao {

    private static final Connection connection = Util.getConnection();

    private final static String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS `users` (\n" +
            "  `id` INT NOT NULL AUTO_INCREMENT,\n" +
            "  `name` VARCHAR(45) NULL,\n" +
            "  `lastName` VARCHAR(45) NULL,\n" +
            "  `age` INT NULL,\n" +
            "        PRIMARY KEY (`id`))";
    private final static String DROP = "DROP TABLE IF EXISTS `users`";
    private final static String ADD = "INSERT INTO `users` (name, lastName, age) values(?,?,?)";
    private final static String REMOVE_USER = "DELETE FROM `users` WHERE `id` = ?";
    private final static String SELECT = "SELECT * FROM `users`";
    private final static String DELETE ="DELETE FROM `users`";

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        try(PreparedStatement  stm = connection.prepareStatement(CREATE_TABLE)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
            System.out.println("Таблица создана");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("Ошибка создания БД" + e.getMessage());
        }
    }

    public void dropUsersTable() {
        try(PreparedStatement  stm = connection.prepareStatement(DROP)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
            System.out.println("Таблица удалена");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("Ошибка удаления БД" + e.getMessage());
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
            System.out.println("User с именем – " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("Ошибка при добавлении пользователя в таблицу" + e.getMessage());
        }
    }
    // собираем preparedStatement из частей и отправляем

    public void removeUserById(long id) {
        try(PreparedStatement  stm = connection.prepareStatement(REMOVE_USER)) {
            connection.setAutoCommit(false);
            stm.setLong(1, id);
            stm.executeUpdate();
            connection.commit();
            System.out.println("Пользователь удален из БД");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("Ошибка при удалении юзера из таблицы" + e.getMessage());
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
            System.out.println("Ошибка получения пользователей из БД" + e.getMessage());
            return null;
        }
        return users;
    }

    public void cleanUsersTable() {
        try (PreparedStatement stm = connection.prepareStatement(DELETE)) {
            connection.setAutoCommit(false);
            stm.executeUpdate();
            connection.commit();
            System.out.println("БД успешно очищена");
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            System.out.println("Ошибка очистки БД" + e.getMessage());
        }
    }
}
