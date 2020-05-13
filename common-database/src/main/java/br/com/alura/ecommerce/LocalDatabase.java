package br.com.alura.ecommerce;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDatabase {

	private final Connection connection;
	
	public LocalDatabase(String databaseName) throws SQLException {
		var url = "jdbc:sqlite:target/" + databaseName + ".db";
		connection = DriverManager.getConnection(url);
	}
	
	public void createIfNotExists(String sql) {
		try {
			connection.createStatement().execute(sql);
		} catch (SQLException e) {
			// TODO
			e.printStackTrace();
		}
	}

	public void update(String sql, String... params) throws SQLException {
		prepare(sql, params).execute();
	}
	
	public ResultSet query(String sql, String... params) throws SQLException {
		return prepare(sql, params).executeQuery();
	}
	
	private PreparedStatement prepare(String sql, String... params) throws SQLException {
		var prepareStatement = connection.prepareStatement(sql);
			for(int i=0; i < params.length; i++) {
				prepareStatement.setString(i+1, params[i]);
			}
			return prepareStatement;
		
	}

	public void close() throws SQLException {
		connection.close();
	}
	
}
