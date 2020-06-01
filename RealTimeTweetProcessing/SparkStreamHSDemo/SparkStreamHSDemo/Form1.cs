using System;
using System.Collections.Generic;
using System.Windows.Forms;
using System.Windows.Forms.DataVisualization.Charting;
using System.Threading;

/// <summary>
/// 
/// </summary>
namespace SparkStreamHSDemo
{
    public partial class Form1 : Form
    {
        const int HATE = 1;

        // data update callbacks
        public delegate void ChartingDelegate(List<chartData> chData);
        public delegate void ChartingWordDelegate(List<wordData> wdData);
        public delegate void PostTweetDelegate(string tweet);

        // charting class contains method for thread
        // does the background work for updating charts and text box
        ChartingClass cc = null;
  
        bool done = true;
        const int LIMIT = 100;

        // thread for background work
        // started in starb_btn click
        Thread chartingThread;
  
        string dt = "";
        static readonly object tight = new object();

        public Form1()
        {
            InitializeComponent();
        }

        private void start_btn_Click(object sender, EventArgs e)
        {
            done = !done;

            if (!done)
            {
                start_btn.Text = "Stop";
                if (this.chartingThread == null)
                {
                    // start background thread
                    // executes chartData in ChartingClass
                    this.chartingThread = new Thread(new ThreadStart(cc.chartData));
                }
         
                // if thread is not alive, start
                // otherwise resume
                if (!chartingThread.IsAlive)
                    this.chartingThread.Start();
                else
                    this.chartingThread.Resume();

         
            }
            else
            {
                // if stop was clicked, suspend thread
                start_btn.Text = "Start";
                if (this.chartingThread != null)
                    this.chartingThread.Suspend();
            }
        }

        /// <summary>
        /// Post hate tweets to text box
        /// </summary>
        /// <param name="tweet"></param>
        private void postTweet(string tweet)
        {
            if (textBox1.InvokeRequired)
            {
                var d = new PostTweetDelegate(postTweet);
                textBox1.Invoke(d, new object[] { tweet });
            }
            else
            {
                textBox1.Text += tweet + Environment.NewLine;
            }
        }

        /// <summary>
        /// Chart hate tweet and normal tweet counts
        /// </summary>
        /// <param name="chData"></param>
        private void chartData(List<chartData> chData)
        {
            if (chart1.InvokeRequired)
            {
                var d = new ChartingDelegate(chartData);

                chart1.Invoke(d, new object[] { chData });
            }
            else
            {
                lock (tight)
                {
                    int numBars = 10;

                    int cntr = 0;
                    this.chart1.Series.Clear();
                    foreach (SparkStreamHSDemo.chartData cData in chData)
                    {

                        // create series and add data points
                        try
                        {
                            Series series = this.chart1.Series.Add("hate");
                            series.Points.Add(cData.hate);
                            series = this.chart1.Series.Add("normal");
                            series.Points.Add(cData.norm);
                        }
                        catch
                        {

                        }
                        cntr++;
                    }

                    // refresh chart
                    this.chart1.ChartAreas[0].RecalculateAxesScale();
                    this.Refresh();
                }
            }
        }

        /// <summary>
        /// Chart tweet word count data
        /// </summary>
        /// <param name="wdData"></param>
        private void chartWords(List<wordData> wdData)
        {
            if (chart1.InvokeRequired)
            {
                var d = new ChartingWordDelegate(chartWords);
                chart2.Invoke(d, new object[] { wdData });
            }
            else
            {
                lock (tight)
                {
                    // initialize number of bars
                    int numBars = 10;

                    int cntr = 0;
                    this.chart2.Series.Clear();

                    // limit number of bars
                    // hardcoded for now
                    // numBars will eventually come from GUI
                    if (numBars < 200)
                    {
                        // iterate through list
                        foreach (wordData cData in wdData)
                        {

                            // create series and add data points
                            try
                            {
                                Series series = this.chart2.Series.Add(cData.word);
                                series.Points.Add(cData.wordCnt);
                            }
                            catch
                            {

                            }
                            cntr++;
                        }
                    }
                    else
                    {
                        MessageBox.Show("Try a smaller number and re-start.");
                        this.chartingThread.Suspend();
                        start_btn.Text = "Start";
                        done = true;
                    }
           

                    // refresh chart
                    this.chart2.ChartAreas[0].RecalculateAxesScale();
                    this.Refresh();
                }
            }
        }

        private void Form1_Load(object sender, EventArgs e)
        {
            // set delegates to their appropriate method
            ChartingDelegate chartDel = new ChartingDelegate(chartData);
            PostTweetDelegate postTweetDel = new PostTweetDelegate(postTweet);
            ChartingWordDelegate chartWordDel = new ChartingWordDelegate(chartWords);

            // 
            cc = new ChartingClass(chartDel, postTweetDel, chartWordDel);

            // only look at recent tweets 
            dt = DateTime.Now.ToString("MM/dd/yyyy H:mm:ss");

            // clear all the series in chart 1 and 2
            this.chart1.Series.Clear();
            this.chart2.Series.Clear();

            // Set chart1 palette
            this.chart1.Palette = ChartColorPalette.Light;

            // Set chart titles
            this.chart1.Titles.Add("Hate Tweet Stats");
            this.chart2.Titles.Add("Tweet Word Stats");

        }

        private void close_btn_Click(object sender, EventArgs e)
        {
            this.Close();
        }

        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            if (chartingThread != null)
            {
                // if thread has been suspended, resume thread so it can be aborted
                if (chartingThread.ThreadState == ThreadState.Suspended)
                    chartingThread.Resume();
                // abort thread
                chartingThread.Abort();

                // loop while still alive
                while (chartingThread.IsAlive)
                { // wait
                }


            }
        }
    }

    public class chartData
    {
        public int hate;
        public int norm;
        public chartData(int hate, int norm)
        {
            this.hate = hate;
            this.norm = norm;
        }
    }

    public class wordData
    {
        public string word;
        public int wordCnt;
        public wordData(string word, int wordCount)
        {
            this.word = word;
            this.wordCnt = wordCount;
        }
    }
}
