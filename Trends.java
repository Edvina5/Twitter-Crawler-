import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Date;
import twitter4j.Location;
import twitter4j.ResponseList;
import twitter4j.Trend;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;

public class TrendingTopics
{
  Trend[] Ireland_topics;
  Date date;
  Connection con;
  Statement st;
  
  public TrendingTopics()
  {
    try
    {
      Class.forName("com.mysql.jdbc.Driver");
      this.con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "");
      this.st = this.con.createStatement();
      System.out.println("Connected to database");
    }
    catch (Exception ex)
    {
      System.out.println("Error:" + ex);
    }
  }
  
  public void Trending(Twitter twitter)
    throws InterruptedException
  {
    for (;;)
    {
      Thread.sleep(2000L);
      try
      {
        ResponseList<Location> locations = twitter.getAvailableTrends();
        
        System.out.println("Showing available trends");
        for (Location location : locations) {
          System.out.println(location.getPlaceName() + " - country: " + location.getCountryName() + " (woeid:" + location.getWoeid() + ")" + "\n");
        }
        Trends trends1 = twitter.getPlaceTrends(23424803);

        this.Ireland_topics = trends1.getTrends();
        this.date = trends1.getAsOf();
        for (int i = 0; i < 10; i++)
        {
          System.out.println(this.Ireland_topics[i].getName());
          System.out.println("Posted at:" + this.date);
        }
      }
      catch (TwitterException te)
      {
        te.printStackTrace();
        System.out.println("Failed to get trends: " + te.getMessage() + "\n");
        System.exit(-1);
      }
      System.out.println("done.");
    }
  }
}
