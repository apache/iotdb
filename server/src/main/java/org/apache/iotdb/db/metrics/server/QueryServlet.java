package org.apache.iotdb.db.metrics.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.iotdb.db.metrics.ui.MetricsPage;
import org.apache.iotdb.db.service.TSServiceImpl;

public class QueryServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	private List<SqlArgument> list = TSServiceImpl.sqlArgumentsList;
	private MetricsPage page;

	public QueryServlet(MetricsPage page) {
		this.page = page;
		this.page.setList(list);
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		resp.setContentType("text/html;charset=utf-8");
		req.setCharacterEncoding("utf-8");
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setIntHeader("refresh", 300);
		PrintWriter out = resp.getWriter();
		out.print(page.render());
		out.flush();
		out.close();
	}

	@Override
	protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		doGet(req, resp);
	}
}
