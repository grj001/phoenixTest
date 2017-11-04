package com.zhiyou.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;


public class PhoenixJDBCTest {
	public static final String URL =
			"jdbc:phoenix:master,slaver1,slaver2:2181";
	
	public static final String DRIVER_CLASS =
			"org.apache.phoenix.jdbc.PhoenixDriver";
	public static final String USER_NAME = "root";
	public static final String PASSWORD = "root";
			
	
	private Connection connection;
	
	public PhoenixJDBCTest() throws Exception{
		Class.forName(DRIVER_CLASS);
		this.connection = DriverManager.getConnection(URL,USER_NAME,PASSWORD);
		
	}
	
	public void findData() throws Exception{
		Statement statement = connection.createStatement();
		String sql = "select * from phoenix_user";
		ResultSet resultSet = statement.executeQuery(sql);
		while(resultSet.next()){
			System.out.println(
					"user_id:"+resultSet.getInt("user_id")
					+",username:"+resultSet.getString(2)
					+",age:"+resultSet.getString("age")
					+",birthday:"+resultSet.getString("birthday")
					);
			
		}
	}
	
	
	public void insertData() throws Exception{
		
		String sql = "upsert into phoenix_user values(?,?,?,?)";
		PreparedStatement statement = 
				connection.prepareStatement(sql);
		Random random = new Random();
		for(int i=0;i<10;i++){
			statement.setInt(1, 3+i);
			statement.setString(2, "user"+i);
			statement.setInt(3, random.nextInt(30)+1);
			statement.setString(
					4
					, "201"+(random.nextInt(10-1)+1)
					+"-"+(1+random.nextInt(12-1))
					+"-"+(random.nextInt(31-1)+1)
					);
			
			
			statement.addBatch();
			System.out.println(statement);
		}
		//自动提交
		connection.setAutoCommit(true);
		statement.executeBatch();
		statement.getConnection().commit();
	}
	
	
	
	
	
	public void cleanUp(){
		if(connection != null){
			try {
				connection.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		PhoenixJDBCTest phoenixJDBCTest = new PhoenixJDBCTest();
//		phoenixJDBCTest.findData();
		phoenixJDBCTest.insertData();
		phoenixJDBCTest.cleanUp();
	}
	
}
