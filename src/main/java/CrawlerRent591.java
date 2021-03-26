import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.bson.Document;
import org.jsoup.Connection;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created with IntelliJ IDEA.
 * User: ChiYuan
 * Date: 2021/03/26 下午 01:42
 * To change this template use File | Settings | File Templates.
 */
public class CrawlerRent591 {
    String api_url = "https://rent.591.com.tw/home/search/rsList?region=<REGION>&firstRow=<PAGESIZE>";
    String[] urlArray = {"https://rent.591.com.tw/?kind=0&region=3", "https://rent.591.com.tw/?kind=0&region=1"};

    public static void main(String[] args) {
        System.setProperty("https.protocols", "TLSv1.2");
        CrawlerRent591 crawlerRent591 = new CrawlerRent591();
        crawlerRent591.start();
    }

    private void start() {
        for (String url : urlArray) {
            try {
                Connection.Response response = SSLHelper.getConnection(url).ignoreContentType(true).method(Connection.Method.GET).timeout(30000).execute();
                Map<String, String> cookies = response.cookies();
                String token = getToken(response.body());
                String total = getTotal(response.body());
                Set<String> articleIdSet = getArticleIdSet(url, token, total, cookies);
                if (!articleIdSet.isEmpty()) {
                    parserArticle(url, articleIdSet);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void parserArticle(String url, Set<String> articleIdSet) {
        String region = getMatchString(url, "(\\d+$)", 1);
        for (String articleID : articleIdSet) {
            String articleUrl = "https://rent.591.com.tw/rent-detail-" + articleID + ".html";
            try {
                System.out.println("Start to parser... " + articleUrl);
                String pageContent = getPageContent(articleUrl);
                if (StringUtils.isNotEmpty(pageContent)) {
                    String id = DigestUtils.md5Hex(articleID);
                    String publisher = getMatchString(pageContent, "data-name=\"([^\"]+)\"", 1);
                    String publisherStatus = getPublisherStatus(articleUrl, publisher, pageContent);
                    String telephone = getTelePhone(pageContent);
                    String statusArea = StringEscapeUtils.unescapeHtml(getMatchString(pageContent, "<ul class=\"attr\">.*?</ul>", 0))
                            .replaceAll("<[^>]+>", "");
                    String type = getMatchString(statusArea, "型態 :(.*?)現況", 1);
                    String situation = getMatchString(statusArea, "現況 :(.*?$)", 1);
                    String gender = getGender(url, pageContent);
                    saveToDatabase(sortOut(id), sortOut(publisher), sortOut(publisherStatus), sortOut(telephone),
                            sortOut(type), sortOut(situation), sortOut(gender), sortOut((region.equals("1") ? "台北" : "新北")));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String getGender(String articleUrl, String pageContent) {
        String gender;
        String genderArea = StringEscapeUtils.unescapeHtml(getMatchString(pageContent, "<ul class=\"clearfix labelList[^>]+>.*?</ul>", 0))
                .replaceAll("<[^>]+>", "");
        genderArea = getMatchString(genderArea, "性別要求.*?$", 0);
        if (StringUtils.isEmpty(genderArea) || genderArea.contains("皆可")) {
            gender = "皆可";
        } else if (genderArea.contains("男")) {
            gender = "男";
        } else if (genderArea.contains("女")) {
            gender = "女";
        } else {
            gender = null;
            System.out.println("無法判定性別" + articleUrl);
        }
        return gender;
    }

    private String getTelePhone(String pageContent) {
        String telephone = getMatchString(pageContent, "data-value=\"([^\"]+)\"", 1);
        if (StringUtils.isEmpty(telephone)) {
            telephone = getMatchString(pageContent, "hidden\"\\s{0,}value=\"([^\"]+)\"", 1);
        }
        return telephone;
    }

    private String getPublisherStatus(String articleUrl, String publisher, String pageContent) {
        String publisherStatus;
        String statusTemp = getMatchString(pageContent, publisher + "<[^>]+>（[^）]+）", 0);
        if (StringUtils.isEmpty(statusTemp)) {
            statusTemp = getMatchString(pageContent, publisher + "<[^>]+>\\([^\\)]+\\)", 0);
        }
        if (statusTemp.contains("屋主")) {
            publisherStatus = "屋主";
        } else if (statusTemp.contains("服務費")) {
            publisherStatus = "仲介";
        } else if (statusTemp.contains("代理人")) {
            publisherStatus = "代理人";
        } else {
            publisherStatus = null;
            System.out.println("無法確定屋主身分" + articleUrl);
        }
        return publisherStatus;
    }

    private String sortOut(String str) {
        if (StringUtils.isNotEmpty(str)) {
            return str.replaceAll(" 轉 ", "#").replaceAll("\\s+|  ", "").replaceAll("社區.*", "").trim();
        } else {
            return str;
        }
    }

    private void saveToDatabase(String id, String publisher, String publisherStatus, String telephone, String type, String situation, String gender, String area) {
        MongoClient mongoClient = new MongoClient("localhost", 27017);
        MongoDatabase mongoDatabase = mongoClient.getDatabase("rent591");
        MongoCollection<Document> rentCollection = mongoDatabase.getCollection("rent591");
        Document rent = new Document("_id", id);
        rent.append("publisher", publisher);
        rent.append("publisherStatus", publisherStatus);
        rent.append("telephone", telephone);
        rent.append("type", type);
        rent.append("situation", situation);
        rent.append("gender", gender);
        rent.append("area", area);
        rentCollection.insertOne(rent);
    }

    private Set<String> getArticleIdSet(String url, String token, String total, Map<String, String> cookies) {
        Set<String> articleIdSet = new TreeSet<String>();
        int firstRow = 0;
        boolean needToGetNextPage;
        String region = getMatchString(url, "(\\d+$)", 1);
        if (StringUtils.isNotEmpty(region)) {
            try {
                do {
                    url = api_url.replace("<REGION>", region).replace("<PAGESIZE>", String.valueOf(firstRow));
                    needToGetNextPage = hasNeedToGetNextPage(firstRow, total);
                    System.out.println("Start To Parser... " + url);
                    HashMap<String, String> headers = new HashMap<>();
                    headers.put("X-CSRF-TOKEN", token);
                    headers.put("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.90 Safari/537.36");
                    Connection.Response response = SSLHelper.getConnection(url).headers(headers).cookies(cookies)
                            .ignoreContentType(true).method(Connection.Method.GET).timeout(30000).execute();
                    if (response.statusCode() == 200) {
                        String pageContent = response.body();
                        JsonObject jsonObject = new Gson().fromJson(pageContent, JsonObject.class);
                        JsonArray articleArray = jsonObject.get("data").getAsJsonObject().get("data").getAsJsonArray();
                        for (JsonElement jsonElement : articleArray) {
                            articleIdSet.add(jsonElement.getAsJsonObject().get("id").getAsString());
                        }
                    }
                    firstRow += 30;
                } while (needToGetNextPage);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return articleIdSet;
    }

    private boolean hasNeedToGetNextPage(int firstRow, String total) {
        if (Integer.valueOf(total) - firstRow > 0) {
            return true;
        } else {
            return false;
        }
    }

    private String getTotal(String content) {
        String total = null;
        try {
            total = getMatchString(content, "共找到(<[^>]+>.*?<[^>]+>)", 1);
            total = total.replaceAll("<[^>]+>", "").replaceAll(",", "").trim();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return total;
    }

    private String getToken(String content) {
        String token = null;
        try {
            token = getMatchString(content, "csrf-token\"?[^\"]+\"([^\"]+)\">", 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return token;
    }

    private String getMatchString(String source, String PATTERN, int group) {
        Matcher matcher = Pattern.compile(PATTERN, Pattern.DOTALL | Pattern.CASE_INSENSITIVE).matcher(source);
        return matcher.find() ? matcher.group(group) : null;
    }

    private String getPageContent(String url) throws Exception {
        String pageContent = "";
        try (CloseableHttpClient httpclient = getDefaultHttpClientBuilder().build()) {
            HttpGet httpGet = new HttpGet(url);
            HttpResponse response = httpclient.execute(httpGet);
            String responseString = EntityUtils.toString(response.getEntity());
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                pageContent = responseString;
            } else {
                System.out.println(response.getStatusLine());
            }
        } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException | IOException e) {
            throw new RuntimeException(e);
        } finally {

        }
        return pageContent;
    }

    private HttpClientBuilder getDefaultHttpClientBuilder() throws Exception {
        return getDefaultHttpClientBuilder("TLSv1.2");
    }

    private HttpClientBuilder getDefaultHttpClientBuilder(String TLSVersion) throws Exception {
        int connectionTimeout = 30000;
        X509TrustManager trustManager = new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] xcs, String s) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] xcs, String s) throws CertificateException {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return null;
            }
        };

        SSLContext sslContext = SSLContext.getInstance(TLSVersion);
        sslContext.init(null, new TrustManager[]{trustManager}, null);
        HttpClientBuilder builder = HttpClientBuilder.create();
        SSLConnectionSocketFactory sslConnectionFactory =
                new SSLConnectionSocketFactory(sslContext.getSocketFactory(), new NoopHostnameVerifier());
        builder.setSSLSocketFactory(sslConnectionFactory);
        Registry<ConnectionSocketFactory> socketFactoryRegistry = RegistryBuilder.<ConnectionSocketFactory>create()
                .register("https", sslConnectionFactory).register("http", new PlainConnectionSocketFactory()).build();
        PoolingHttpClientConnectionManager httpClientConnectionManager =
                new PoolingHttpClientConnectionManager(socketFactoryRegistry);
        SocketConfig socketConfig = SocketConfig.custom().setSoTimeout(connectionTimeout).build();
        httpClientConnectionManager.setMaxTotal(Integer.MAX_VALUE);
        httpClientConnectionManager.setDefaultMaxPerRoute(Integer.MAX_VALUE);
        httpClientConnectionManager.setDefaultSocketConfig(socketConfig);
        builder.setConnectionManager(httpClientConnectionManager);
        builder.setKeepAliveStrategy(new ConnectionKeepAliveStrategy() {
            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                return 20 * 1000;
            }
        });
        return builder;
    }
}