import com.mysql.jdbc.PreparedStatement;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import twitter4j.FilterQuery;
import twitter4j.GeoLocation;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.URLEntity;
import twitter4j.User;
import twitter4j.conf.ConfigurationBuilder;

public class SimpleStream
{
  protected static final String Log = null;
  static Connection con;
  static Statement st;
  private static double[][] geo = { { -180.0D, -90.0D }, 
    { 180.0D, 90.0D } };
  public static final String URL = "URLS";
  static String screen_name;
  static String name;
  static long user_id;
  static String location;
  static int follower_count;
  static int friend_count;
  static int status_count;
  static String profileLocation;
  static String weekday;
  static String month;
  static String day;
  static String hour;
  static String minute;
  static String second;
  static Long tweet_id;
  static Long retweeted_Id;
  static String retweeted_text;
  static String content;
  static String tweet_country;
  static float tweet_longtitude;
  static float tweet_latitude;
  static int favorite_count;
  static int retweet_count;
  
  public void Autorize(ConfigurationBuilder cb)
    throws InterruptedException
  {
    cb.setDebugEnabled(true);
    cb.setOAuthConsumerKey("***************");
    cb.setOAuthConsumerSecret("***************");
    cb.setOAuthAccessToken("***************");
    cb.setOAuthAccessTokenSecret("***************");
  }
  
  public void Java2MySql()
  {
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
      con = DriverManager.getConnection("jdbc:mysql://***************", "*******", "**********");
      st = con.createStatement();
      System.out.println("Connected to database");
    }
    catch (Exception ex)
    {
      System.out.println("Error:" + ex);
    }
  }
  
  public static void main(String[] args)
    throws InterruptedException
  {
    try
    {
      SimpleStream ss = new SimpleStream();
      ss.Java2MySql();
      

      ConfigurationBuilder cb = new ConfigurationBuilder();
      Authorization au = new Authorization();
      au.Autorize(cb);
      

      TwitterStream twitter = new TwitterStreamFactory(cb.build()).getInstance();
      

      StatusListener listener = new StatusListener()
      {
        public void onException(Exception arg0) {}
        
        public void onDeletionNotice(StatusDeletionNotice arg0) {}
        
        public void onScrubGeo(long arg0, long arg1) {}
        
        public void onStatus(Status status)
        {
          SimpleStream.screen_name = status.getUser().getScreenName();
          

          SimpleStream.name = status.getUser().getName();
          

          SimpleStream.user_id = status.getUser().getId();
          


          SimpleStream.follower_count = status.getUser().getFollowersCount();
          

          SimpleStream.friend_count = status.getUser().getFriendsCount();
          

          SimpleStream.status_count = status.getUser().getStatusesCount();
          





          PreparedStatement stmt = null;
          try
          {
            stmt = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT IGNORE INTO users6 (User_ID,Screen_name,Name,Follower_count,Favorite_count,Status_count) VALUES (?,?,?,?,?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt.setLong(1, SimpleStream.user_id);
            stmt.setString(2, SimpleStream.screen_name);
            stmt.setString(3, SimpleStream.name);
            stmt.setInt(4, SimpleStream.follower_count);
            stmt.setInt(5, SimpleStream.friend_count);
            stmt.setInt(6, SimpleStream.status_count);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          SimpleStream.tweet_id = Long.valueOf(status.getId());
          

          SimpleStream.content = status.getText();
          

          Date date = status.getCreatedAt();
          


          String realDate = date.toString();
          



          SimpleStream.weekday = realDate.substring(0, 3);
          


          SimpleStream.month = realDate.substring(4, 8);
          

          SimpleStream.day = realDate.substring(8, 11);
          


          SimpleStream.hour = realDate.substring(11, 13);
          

          SimpleStream.minute = realDate.substring(14, 16);
          

          SimpleStream.second = realDate.substring(17, 19);
          


          PreparedStatement stmt5 = null;
          try
          {
            stmt5 = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT INTO timestamp6 (Weekday, Month, Tweet_ID,Day, Hour, Minute, Second) VALUES (?,?,?,?,?,?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt5.setString(1, SimpleStream.weekday);
            stmt5.setString(2, SimpleStream.month);
            stmt5.setLong(3, SimpleStream.tweet_id.longValue());
            stmt5.setString(4, SimpleStream.day);
            stmt5.setString(5, SimpleStream.hour);
            stmt5.setString(6, SimpleStream.minute);
            stmt5.setString(7, SimpleStream.second);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt5.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          SimpleStream.tweet_latitude = (float)status.getGeoLocation().getLatitude();
          

          SimpleStream.tweet_longtitude = (float)status.getGeoLocation().getLongitude();
          

          SimpleStream.favorite_count = status.getFavoriteCount();
          



          SimpleStream.retweet_count = status.getRetweetCount();
          













          SimpleStream.tweet_country = status.getPlace().getCountry();
          
          SimpleStream.location = status.getUser().getLocation();
          


          PreparedStatement stmt2 = null;
          try
          {
            stmt2 = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT IGNORE INTO tweets6 (Tweet_ID, Content, Retweet_count, Favorite_count,Tweet_longtitude, Tweet_latitude, Stream, User_ID, Location ) VALUES (?,?,?,?,?,?,?,?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt2.setLong(1, SimpleStream.tweet_id.longValue());
            stmt2.setString(2, SimpleStream.content);
            stmt2.setInt(3, SimpleStream.retweet_count);
            stmt2.setInt(4, SimpleStream.favorite_count);
            stmt2.setFloat(5, SimpleStream.tweet_longtitude);
            stmt2.setFloat(6, SimpleStream.tweet_latitude);
            
            stmt2.setString(7, "S");
            stmt2.setLong(8, SimpleStream.user_id);
            stmt2.setString(9, SimpleStream.tweet_country);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt2.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          PreparedStatement stmt7 = null;
          try
          {
            stmt7 = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT IGNORE INTO locations5 (User_ID,Location) VALUES (?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt7.setLong(1, SimpleStream.user_id);
            stmt7.setString(2, SimpleStream.location);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt7.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          HashtagEntity[] hashtags = status.getHashtagEntities();
          
          hashtags = status.getHashtagEntities();
          
          String hts = "";
          if (hashtags != null) {
            for (HashtagEntity hte : hashtags) {
              hts = hts + " " + hte.getText();
            }
          }
          PreparedStatement stmt3 = null;
          try
          {
            stmt3 = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT INTO tags6 (Tweet_ID,Tag) VALUES (?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt3.setLong(1, SimpleStream.tweet_id.longValue());
            stmt3.setString(2, hts);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt3.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          URLEntity[] urls = status.getURLEntities();
          

          String urlStr = "";
          if (urls != null) {
            for (URLEntity ue : urls) {
              urlStr = urlStr + " " + ue;
            }
          }
          urlStr = urlStr.substring(24, 46);
          



          PreparedStatement stmt4 = null;
          try
          {
            stmt4 = (PreparedStatement)SimpleStream.con.prepareStatement(
              "INSERT INTO urls6 (Tweet_ID,url ) VALUES (?,?)");
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt4.setLong(1, SimpleStream.tweet_id.longValue());
            stmt4.setString(2, urlStr);
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
          try
          {
            stmt4.execute();
          }
          catch (SQLException e)
          {
            e.printStackTrace();
          }
        }
        
        public void onTrackLimitationNotice(int arg0) {}
        
        public void onStallWarning(StallWarning arg0) {}
      };
      FilterQuery fq = new FilterQuery();
      


      String[] lang = { "en" };
      String[] keywords = { "nba" };
      




      fq.locations(geo).language(lang);
      twitter.addListener(listener);
      
      twitter.filter(fq);
    }
    catch (InterruptedException e)
    {
      Date error_date = new Date();
      System.out.println("Error:" + e);
      System.out.println("The date of an error: " + error_date.toString());
    }
  }
}
