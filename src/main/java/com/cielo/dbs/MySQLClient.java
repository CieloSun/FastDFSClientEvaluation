package com.cielo.dbs;

import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class MySQLClient {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String URL = "jdbc:mysql://ws.cielosun.xyz:32771/cielo";
    static final String USER = "root";
    static final String PASSWORD = "hahaschool";
    private BasicDataSource dataSources = new BasicDataSource();

    public MySQLClient(int numbers) throws Exception {
        dataSources.setDriverClassName(JDBC_DRIVER);
        dataSources.setUrl(URL);
        dataSources.setUsername(USER);
        dataSources.setPassword(PASSWORD);
        dataSources.setMaxTotal(numbers);
        dataSources.setMaxIdle(numbers);
        dataSources.setMaxWaitMillis(3000);
        dataSources.setRemoveAbandonedTimeout(100);
        dataSources.setRemoveAbandonedOnBorrow(true);
        dataSources.setRemoveAbandonedOnMaintenance(true);
    }

    private Connection getConnection() throws Exception {
        return dataSources.getConnection();
    }

    public void upload(String dataId, byte[] bytes) {
        try {
            Connection connection = getConnection();
            PreparedStatement preparedStatement = connection.prepareStatement("insert into test(dataId, data) values(?,?)");
            preparedStatement.setString(1, dataId);
            preparedStatement.setBytes(2, bytes);
            preparedStatement.execute();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public byte[] download(String dataId) throws Exception {
        Connection connection = getConnection();
        PreparedStatement preparedStatement = connection.prepareStatement("select data from test where dataId=?");
        preparedStatement.setString(1, dataId);
        ResultSet resultSet = preparedStatement.executeQuery();
        resultSet.next();
        byte[] bytes = resultSet.getBytes(1);
        connection.close();
        return bytes;
    }
}
