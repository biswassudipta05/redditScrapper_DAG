# redditScrapper_DAG

redditScrapper_DAG is an extension of [redditScrapper][1] which uses Apache Airflow for it's scheduling and execution.

## Important

You will need the files of [redditScrapper][1] present in home directory of your airflow installtion or the directory where the sysvar `AIRFLOW_HOME` points.

Databse initilization, populating the database with the list of subreddits and generating report. All needs to be done using the scripts of [redditScrapper][1] 

**The Database should be already initialised and populated with the list of subreddits before running this DAG.**

Kindly refer to the readme of [redditScrapper][1] for its installlation and usage.

## Future Development
This DAG requires much more development and currently it's just the basic initial version.

[1]: https://github.com/biswassudipta05/redditScrapper