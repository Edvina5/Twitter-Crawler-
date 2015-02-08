import twitter4j.conf.ConfigurationBuilder;

public class Authorization
{
  public void Autorize(ConfigurationBuilder cb)
    throws InterruptedException
  {
    cb.setDebugEnabled(true);
    cb.setOAuthConsumerKey("***************");
    cb.setOAuthConsumerSecret("***************");
    cb.setOAuthAccessToken("****************");
    cb.setOAuthAccessTokenSecret("***************");
  }
}
