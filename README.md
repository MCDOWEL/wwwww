package com.dao;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

import com.bean.Customer;
import com.bean.Product;
import com.util.Datautil;

public class ProductDAO {
	
	public boolean addCustomerAndProduct(Customer cust) throws SQLException {
		
		Connection con = Datautil.getConnection();
		
		String sql = "insert into TBL_CUSTOMER_1341416 values(?,?,?,?)";
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ps.setLong(1, cust.getCustomerId());
		ps.setString(2, cust.getCustomerName());
		ps.setString(3, cust.getAddress());
		ps.setLong(4, cust.getMobile());
		
		int n = ps.executeUpdate();
		
		if(n>0) 
		{
			if(cust.getProductlst().size() != 0)
			{
				int count = 0;
				System.out.println("jk");
				for(Product p : cust.getProductlst()) 
				{
					
				sql = "insert into TBL_PRODUCT_1341416 values(?,?,?,?,?,?)";
				
				ps = con.prepareStatement(sql);
				
				ps.setLong(1, p.getProductId());
				ps.setString(2, p.getProductName());
				ps.setString(3, p.getProductType());
				ps.setDouble(4, p.getPrice());
				ps.setLong(5, p.getQuantity());
				ps.setLong(6, p.getCustomerId());
				
				 n = ps.executeUpdate();
				 count++;
				}
				System.out.println("NO.Of products updated :"+count);
			}
			
			return true;
		}
		
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		return false;
		
	}
	

	
	public ArrayList<Product> getProductByCustomer(long customerId) throws SQLException{
		
		ArrayList<Product> productlist = new ArrayList<Product>();
		Connection con = Datautil.getConnection();
		
		String sql = "SELECT productid,productname,producttype,price,quantity,TBL_CUSTOMER_1341416.customerid from TBL_CUSTOMER_1341416 INNER JOIN " + 
				"  TBL_PRODUCT_1341416 ON TBL_CUSTOMER_1341416.customerid=TBL_PRODUCT_1341416.customerid where TBL_CUSTOMER_1341416.customerid="+customerId;
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			
			long pid =rs.getLong(1);
			String name = rs.getString(2);
			String type = rs.getString(3);
			double price = rs.getDouble(4);
			long quantity = rs.getLong(5);
			long cid = rs.getLong(6);
			
			Product p = new Product(pid, name, type, price, quantity, cid);
			
			productlist.add(p);
			
		}
		
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		return productlist;
	}
	
	
	
	public HashMap<Long, Integer> getProductCountByCustomer() throws SQLException{
		
		HashMap<Long, Integer> hm = new HashMap<Long, Integer> ();
		Connection con = Datautil.getConnection();
		
		String sql = "SELECT TBL_CUSTOMER_1341416.customerid,count(*) from TBL_CUSTOMER_1341416 INNER JOIN " + 
				"  TBL_PRODUCT_1341416 ON TBL_CUSTOMER_1341416.customerid=TBL_PRODUCT_1341416.customerid" + 
				"  GROUP BY TBL_CUSTOMER_1341416.customerid ORDER BY TBL_CUSTOMER_1341416.customerid";
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			
			long id = rs.getLong(1);
			int count = rs.getInt(2);
			
			hm.put(id, count);
			
		}
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		return hm;
	}
	
	
	public boolean deleteCustomer(long customerId) throws SQLException {
		
		Connection con = Datautil.getConnection();
		
		String sql = "delete from TBL_PRODUCT_1341416 where customerid="+customerId;
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		int n = ps.executeUpdate();
		
		if(n>0)
		{
			
			sql = "delete from TBL_CUSTOMER_1341416 where customerid="+customerId;
			
			ps= con.prepareStatement(sql);
			
			int n1 = ps.executeUpdate();
			
			if(n1>0)
			
			{
				return true;
			}
		}
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		
		return false;
	}
	
	
	public Customer getCustomerDetails(long customerId) throws SQLException {
		
		Customer cust = null ;
		Connection con = Datautil.getConnection();
		
		String sql = "select * from TBL_CUSTOMER_1341416 where customerid="+customerId;
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			
			long id = customerId;
			String name = rs.getString(2);
			String address = rs.getString(3);
			long number = rs.getLong(4);
			
			 cust = new Customer(id, name, address, number);
			
			
		}
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		return cust;
	}
	
	
	
	public boolean updateProductQuantityAndPrice(long productId,long quantity,double price) throws SQLException {
		
		Connection con = Datautil.getConnection();
		
		String sql = "UPDATE TBL_PRODUCT_1341416 SET quantity=?,price=? where productid="+productId;
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ps.setLong(1, quantity);
		ps.setDouble(2, price);
		
		int n = ps.executeUpdate();
		
		if(n>0) {
			return true;
		}
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		
		return false;
	}
	
	
	public void getCountByType() throws SQLException {
		
		Connection con = Datautil.getConnection();
		
		String sql = "SELECT producttype,count(*) from TBL_CUSTOMER_1341416 INNER JOIN " + 
				"  TBL_PRODUCT_1341416 ON TBL_CUSTOMER_1341416.customerid=TBL_PRODUCT_1341416.customerid" + 
				"  GROUP BY producttype";
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next())
		{
			System.out.println(rs.getString(1)+" Type has count of :"+rs.getInt(2));	
		}
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		
	}
	
	
	public Customer getCustomerAndProduct(long customerId) throws SQLException {
		
		Customer cust = null ;
		
		Connection con = Datautil.getConnection();
		
		String sql = "select * from TBL_CUSTOMER_1341416 where customerid ="+customerId;
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		
		ResultSet rs = ps.executeQuery();
		
		
		
		while(rs.next()) {
			
			long id = rs.getLong("customerid");
			String name = rs.getString("customername");
			String address = rs.getString("address");
			long number = rs.getLong("mobile");
			
			Customer c1 = new Customer(id, name, address, number);
			
			cust = c1;
			
		}
		
		
		
		sql = "select * from TBL_PRODUCT_1341416 where customerid ="+customerId;
		
		ps = con.prepareStatement(sql);
		
		 rs = ps.executeQuery();
		
		ArrayList<Product> productlist = new ArrayList<Product> ();
		
		
		
		while(rs.next()) {
			
			long id = rs.getLong(1);
			String name = rs.getString(2) ;
			String type = rs.getString(3);
			double price =rs.getDouble(4);
			long quantity = rs.getLong(5);
			long customerid = customerId;
			
			
			Product p = new Product(id, name, type, price, quantity, customerid);
			
			productlist.add(p);
			
			
		}
		cust.setProductlst(productlist);
		Datautil.closeConnection(con);
		Datautil.closeStatement(ps);
		
		return cust;
		
	}
	
	
	
	public boolean registerCustomer(Customer cust) throws SQLException {
		
		Connection con = Datautil.getConnection();
		
		
		String sql = "select mobile from TBL_CUSTOMER_1341416";
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			if(rs.getLong(1) == cust.getMobile())
			{
				return false;
			}
			
		}
		
		sql = "insert into TBL_CUSTOMER_1341416 values(customer_seq.NEXTVAL,?,?,?)";
		
		 ps = con.prepareStatement(sql);
		 
		 ps.setString(1, cust.getCustomerName());
		 ps.setString(2, cust.getAddress());
		 ps.setLong(3, cust.getMobile());
		 int n = ps.executeUpdate();
		 
		 Datautil.closeConnection(con);
		 Datautil.closeStatement(ps);
		 
		 
		 if(n>0) {
			 return true;
		 }

		return false;
		
		
	}
	
	
	public long getrandomvalue(Customer cust) throws SQLException {
		
		long id =0;
		
		Connection con = Datautil.getConnection();
		
		
		String sql = "select customerid from TBL_CUSTOMER_1341416 where mobile = "+cust.getMobile();
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			
			 id = rs.getLong(1);
			
		}
		 Datautil.closeConnection(con);
		 Datautil.closeStatement(ps);
		
		return id;
	}
	
	
	public ArrayList<Customer> viewCustomer() throws SQLException{
		
		ArrayList<Customer> req = new ArrayList<Customer>();
		
		Connection con = Datautil.getConnection();
		
		String sql = "SELECT * from TBL_CUSTOMER_1341416 ";
		
		PreparedStatement ps = con.prepareStatement(sql);
		
		ResultSet rs = ps.executeQuery();
		
		while(rs.next()) {
			
			long id = rs.getLong(1);
			String name = rs.getString(2);
			String address = rs.getString(3);
			long mobile = rs.getLong(4);
			
			Customer cus = new Customer(id, name, address, mobile);
			
			req.add(cus);
			
		}
		
		 Datautil.closeConnection(con);
		 Datautil.closeStatement(ps);
		 return req;

	}
	
	
	
	
	
	
	
	
	
	
	
	
}
