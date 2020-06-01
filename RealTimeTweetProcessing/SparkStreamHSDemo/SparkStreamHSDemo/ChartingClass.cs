using System;
using System.Collections.Generic;
using System.Data.Odbc;

namespace SparkStreamHSDemo
{
    class ChartingClass
    {
        bool done = false;
        Form1.ChartingDelegate chartDel;
        Form1.ChartingWordDelegate chartWordDel;
        Form1.PostTweetDelegate postTweetDel;
        OdbcConnection conn = new OdbcConnection();
        OdbcCommand cmd = new OdbcCommand();
        OdbcCommand ucmd = new OdbcCommand();
        OdbcDataReader dr;
        readonly object tight = new object();
        public int max = 10;

        public int maxBars
        {
            set
            {
                max = value;
            }
            get
            {
                return max;
            }
        }

        public ChartingClass(Form1.ChartingDelegate cd, Form1.PostTweetDelegate pt, Form1.ChartingWordDelegate wd)
        {
            chartDel = cd;
            postTweetDel = pt;
            chartWordDel = wd;
            // open database connection
            conn.ConnectionString = "DSN=twitter";
            ucmd.Connection = conn;
            cmd.Connection = conn;
            if (conn.State == System.Data.ConnectionState.Closed)
                conn.Open();
        }

        public void postTweet()
        {
            string sql = "SELECT id, tweet ";
            sql += "FROM twitter.tweet_t ";
            sql += "WHERE prediction = 1 ";
            sql += "AND posted IS NULL ";

            lock (tight)
            {
                cmd.CommandText = sql;
                dr = cmd.ExecuteReader();

                while (dr.Read())
                {
                    postTweetDel.Invoke(dr["tweet"].ToString());

                    sql = "UPDATE twitter.tweet_t SET posted = 1 WHERE id = " + dr["id"].ToString();
                    ucmd.CommandText = sql;
                    ucmd.ExecuteNonQuery();
                }
                // close database connection
                dr.Close();
            }
        }
        public void chartData()
        {
            while (!done)
            {
                // use line graph for this
                string sql = "SELECT hate, norm ";
                sql += "FROM (SELECT COUNT(tweet) hate FROM twitter.tweet_t WHERE prediction = 1) as hate, ";
                sql += "(SELECT COUNT(tweet) norm FROM twitter.tweet_t WHERE prediction = 0 ) as norm ";
                lock (tight)
                {
                    cmd.CommandText = sql;
                    dr = cmd.ExecuteReader();

                    List<SparkStreamHSDemo.chartData> chData = new List<chartData>();

                    int cntr = 0;
                    while (dr.Read())
                    {
                        if (cntr < maxBars)
                            chData.Add(new chartData(Convert.ToInt32(dr["hate"]), Convert.ToInt32(dr["norm"])));
                        else
                            break;
                        cntr++;
                    }
                    // close database connection
                    dr.Close();
                    chartDel.Invoke(chData);

                    dr.Close();

                    postTweet();

                    chartWordCount();
                }
            }
        }

        public void chartWordCount()
        {
  
            // use line graph for this
            string sql = "SELECT word, SUM(wd_count) cnt ";
            sql += "FROM word_count_t ";
            sql += "GROUP BY word ORDER BY SUM(wd_count) DESC";
            lock (tight)
            {


                cmd.CommandText = sql;
                dr = cmd.ExecuteReader();


                List<wordData> chData = new List<wordData>();

                int cntr = 0;
                while (dr.Read())
                {
                    if (cntr < maxBars)
                        chData.Add(new wordData(dr["word"].ToString(), Convert.ToInt32(dr["cnt"])));
                    else
                        break;
                    cntr++;
                }
                // close database connection
                dr.Close();
                chartWordDel.Invoke(chData);
            }
    
        }
    }
}
