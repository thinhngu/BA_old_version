package crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;


import org.json.*;

import analytics.MongoQueries;
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler extends WebCrawler {
	// patterns for media files, why also css?
	Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g" 
			+ "|png|tiff?|mid|mp2|mp3|mp4" 
			+ "|wav|avi|mov|mpeg|ram|m4v|pdf"
			+ "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
	
	
	 /**
     * This method receives two parameters. The first parameter is the page in
     * which we have discovered this new url and the second parameter is the new
     * url. You should implement this function to specify whether the given url
     * should be crawled or not (based on your crawling logic). In this example,
     * we are instructing the crawler to ignore urls that have css, js, git, ...
     * extensions and to only accept urls that start with
     * "http://www.ics.uci.edu/". In this case, we didn't need the referringPage
     * parameter to make the decision.
     */
    @Override
    public boolean shouldVisit(Page referringPage, WebURL url) {

        String href = url.getURL().toLowerCase();
        return !filters.matcher(href).matches()
                && !href.contains("twitter.com")
                && !href.contains("wikipedia.org")
                && !href.contains("facebook.com")
                && !href.contains("twitter.com")
                && !href.contains("instagram.com");

    }

    /**
     * This function is called when a page is fetched and ready to be processed
     * by your program.
     */
    @Override
    public void visit(Page page) {
        Connection myConn = null;
        Statement myStmt = null;
        ResultSet myRs = null;

        String url = "values " + "(" + "'" + page.getWebURL().getURL() + "'"
                + ")";

        String UrlforJson = page.getWebURL().getURL();
        Pattern jsonp = Pattern.compile("\\.json");
        Matcher JSONmatcher = jsonp.matcher(UrlforJson);

        Pattern xmlp = Pattern.compile("\\.xml");
        Matcher XMLmatcher = xmlp.matcher(UrlforJson);

        Pattern rdfp = Pattern.compile("\\.rdf");
        Matcher RDFmatcher = rdfp.matcher(UrlforJson);

        Pattern ttlp = Pattern.compile("\\.ttl");
        Matcher TTLmatcher = ttlp.matcher(UrlforJson);

        Pattern yamlp = Pattern.compile("\\.yaml");
        Matcher YAMLmatcher = yamlp.matcher(UrlforJson);

        Pattern jsonldp = Pattern.compile("\\.jsonld");
        Matcher JSONLDmatcher = jsonldp.matcher(UrlforJson);

        Pattern owlp = Pattern.compile("\\.owl");
        Matcher OWLmatcher = owlp.matcher(UrlforJson);

        Pattern ntp = Pattern.compile("\\.nt");
        Matcher NTmatcher = ntp.matcher(UrlforJson);

        Pattern qtp = Pattern.compile("\\.qt");
        Matcher QTmatcher = qtp.matcher(UrlforJson);

        Pattern mdp = Pattern.compile("\\.md");
        Matcher MDmatcher = mdp.matcher(UrlforJson);

        Pattern markdownp = Pattern.compile("\\.markdown");
        Matcher Markdownmatcher = markdownp.matcher(UrlforJson);
        if (JSONmatcher.find()) {
            MongoQueries mongowriter = new MongoQueries();
            mongowriter.insertJson(url, root);
            mongowriter.close();
        }

        if (JSONmatcher.find() || XMLmatcher.find() || RDFmatcher.find()
                || YAMLmatcher.find() || JSONLDmatcher.find()
                || OWLmatcher.find() || NTmatcher.find() || QTmatcher.find()
                || MDmatcher.find() || Markdownmatcher.find()) {

       

            try {
                // 1. Get a connection to database
                myConn = DriverManager.getConnection(
                        "jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306/crawler",
                        "thinh", "thinh");

                // 2. Create a statement
                myStmt = myConn.createStatement();

                // 3. Execute SQL query
                String sql = "insert into crawled_files" + " (links)" + url;
                myStmt.executeUpdate(sql);
                // myRs = myStmt.executeQuery("select * from employees");//

            } catch (Exception exc) {
                exc.printStackTrace();
            } finally {
                if (myRs != null) {
                    try {
                        myRs.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                if (myStmt != null) {
                    try {
                        myStmt.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }

                if (myConn != null) {
                    try {
                        myConn.close();
                    } catch (SQLException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }

        }
     
        try {
            // 1. Get a connection to database
            myConn = DriverManager.getConnection(
                    "jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306/crawler",
                    "thinh", "thinh");

            // 2. Create a statement
            myStmt = myConn.createStatement();

            // 3. Execute SQL query
            String sql = "insert into crawled_url" + " (links)" + url;
            myStmt.executeUpdate(sql);
            // myRs = myStmt.executeQuery("select * from employees");//

        } catch (Exception exc) {
            exc.printStackTrace();
        } finally {
            if (myRs != null) {
                try {
                    myRs.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (myStmt != null) {
                try {
                    myStmt.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (myConn != null) {
                try {
                    myConn.close();
                } catch (SQLException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }



	
	

}
