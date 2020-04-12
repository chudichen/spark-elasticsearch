package com.michael.jdbc;

import java.sql.*;

/**
 * @author Michael Chu
 * @since 2020-04-08 18:43
 */
public class PostgreJDBC {

    public static void main(String[] args) {
        String url = "jdbc:postgresql://localhost:5432/metastore";
        String user = "hive";
        String password = "hive";

        try (Connection con = DriverManager.getConnection(url, user, password);
             Statement st = con.createStatement();
             ResultSet rs = st.executeQuery("SELECT VERSION()")) {

            if (rs.next()) {
                System.out.println(rs.getString(1));
            }

        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }
}
