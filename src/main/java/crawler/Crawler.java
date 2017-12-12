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
import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class Crawler extends WebCrawler {
    Pattern filters = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g"
            + "|png|tiff?|mid|mp2|mp3|mp4" + "|wav|avi|mov|mpeg|ram|m4v|pdf"
            + "|rm|smil|wmv|swf|wma|zip|rar|gz))$");
    String Urls = "";

    public static AtomicInteger linkCounter = new AtomicInteger(); // or use
                                                                   // links.size()
    static List<WebURL> links = new CopyOnWriteArrayList<WebURL>();

    @Override
    public void handlePageStatusCode(final WebURL webUrl, int statusCode,
            String statusDescription) {
        linkCounter.incrementAndGet();
        links.add(webUrl);
    }

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
        return !filters.matcher(href).matches();

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
        
        String url ="values " + "(" + "'" + page.getWebURL().getURL() + "'" + ")";
        String UrlforJson = page.getWebURL().getURL();
        Pattern pattern = Pattern.compile(".json");
        Matcher matcher = pattern.matcher(UrlforJson);
        if(matcher.find()) {
            
            URI uri = null;
            try {
                uri = new URI(UrlforJson);
            } catch (URISyntaxException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            JSONTokener tokener = null;
            try {
                tokener = new JSONTokener(uri.toURL().openStream());
            } catch (MalformedURLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            JSONObject root = new JSONObject(tokener);
            
            if(root.has("swagger")){
                try {
                    // 1. Get a connection to database
                    myConn = DriverManager.getConnection("jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306/crawler", "thinh" , "thinh");
                    
                    // 2. Create a statement
                    myStmt = myConn.createStatement();
                    
                    // 3. Execute SQL query
                    String sql ="insert into SWAGGERFILE" + " (LinkToSwagger)" +
                               url;
                    myStmt.executeUpdate(sql);
                    //myRs = myStmt.executeQuery("select * from employees");//
                    
                
                }
                catch (Exception exc) {
                    exc.printStackTrace();
                }
                finally {
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
        
        
        try {
            // 1. Get a connection to database
            myConn = DriverManager.getConnection("jdbc:mysql://aifb-ls3-vm1.aifb.kit.edu:3306/crawler", "thinh" , "thinh");
            
            // 2. Create a statement
            myStmt = myConn.createStatement();
            
            // 3. Execute SQL query
            String sql ="insert into URL" + " (link)" +
                       url;
            myStmt.executeUpdate(sql);
            //myRs = myStmt.executeQuery("select * from employees");//
            
        
        }
        catch (Exception exc) {
            exc.printStackTrace();
        }
        finally {
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

    @Override
    public void onBeforeExit() {
       /* for (int i = 0; i < links.size(); i++) {
            System.out.println(links.get(i));
        }*/
    }

}
