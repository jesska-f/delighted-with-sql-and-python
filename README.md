# Using SQL and Python to send surveys to customers using Delighted
This repo contains the scripts that accompany the blog post which explains the process behind using SQL and Python, along with the Delighted API to send email surveys to customers, with additional information that will enhance the reporting on the survey responses.  

It makes use of environment variables to store the API key and the database connection string.  

Set the following enviroment variables according to your API Key and DB details:

`DW_CONNECTION_STRING: "postgresql://username:password@databse_url:5439/db_name"`

`DELIGHTED_API_KEY: "abc123thisisnotarealkey"`  



The other environment variables needed for the g2pg package are described in this repo - https://github.com/jesska-f/g2pg
