- Logger_Final.ipynb: <i>initial attempt at generating log files. Values weren't entirely random.</i>

- SparkSQL_logfile_parsing.ipynb: <i>performs analyses on the dataset using SparkSQL library and records execution times.</i>

- datalog.csv: <i>a sample of the dataset we are using. Actual file too large to upload (~1.12 GB).</i>

- .sql files: <i>creates the tables into which our logfile generator outputs random values.</i>

- logFileGen.py: <i>the logfile generator we used to create a randomized dataset. </i>

- python_logfile_parsing.ipynb: <i>performs analyses on the dataset using no external library, and records execution times.</i>


Spark SQL offers increased querying by loading files into a Spark Dataframe. Performance is benchmarked for running the same analyses with/without the Spark SQL library.
