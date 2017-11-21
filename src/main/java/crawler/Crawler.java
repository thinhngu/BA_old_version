package crawler;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

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
    static List links = new CopyOnWriteArrayList();

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

        String url = page.getWebURL().getURL();
        System.out.println("Visited: " + url);

        if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText();
            String html = htmlParseData.getHtml();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();

            Urls = text + "/n";
            System.out.println("Text length: " + text.length());
            System.out.println("Html length: " + html.length());
            System.out.println("Number of outgoing links: " + links.size());
        }
    }

    @Override
    public void onBeforeExit() {
        for (int i = 0; i < links.size(); i++) {
            System.out.println(links.get(i));
        }
    }

}
