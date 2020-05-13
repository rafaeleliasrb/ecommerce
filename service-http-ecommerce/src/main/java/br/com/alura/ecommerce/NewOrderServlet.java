package br.com.alura.ecommerce;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;

public class NewOrderServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;
	
	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();
	
	@Override
	public void destroy() {
		super.destroy();
		orderDispatcher.close();
	}
	
	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			var emailAddress = req.getParameter("email");
			var amount = new BigDecimal(req.getParameter("amount"));
			var uuid = req.getParameter("uuid");
			
			var order = new Order(uuid, amount, emailAddress);
		
			try(var database = new OrderDatabase()) {
				if(database.saveNew(order)) {
					orderDispatcher.send("ECOMMERCE_NEW_ORDER", 
							emailAddress, 
							new CorrelationId(NewOrderServlet.class.getSimpleName()),
							order);
					
					System.out.println("New order sent successfully");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("New order sent");
				}
				else {
					System.out.println("Old order received");
					resp.setStatus(HttpServletResponse.SC_OK);
					resp.getWriter().println("Old order received");
				}
			}
			
		} catch (InterruptedException | ExecutionException | SQLException e) {
			throw new ServletException(e);
		}
	}
}
