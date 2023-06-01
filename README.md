# GLOBANT CHALLENGE

Hi! In this repo you will find the next files:
- `db.yaml`: In this file the database information must be defined.
- `main.py`: This is the file that contains the API code.
- `requests.sh`: curl requests to upload a file using bash.
- `spark_loader.py`: Spark code that read the CSV data and load it into MySQL.
- `sql_querys.sql`: In this file are the queries that I did for the challenge.

To execute the code you will need:
- Python 3.8.0
-- Flask
-- Yaml 
- Spark 3.0.0+
- MySQL 8.0

Also the repo contains 2 folders, `SourceFiles` and `TargetFiles`. The `SourceFiles` folder contains the CSV files to load into MySQL. I suggest to move into the folder with your terminal and execute the Post request. In the file `requests.sh` are the example of how to execute the request using curl. The `TargetFiles` is where the API will write the files and then Spark will read them from there.

In the `main.py` and `spark_loader.py` exists a variable named `USER_PATH`, please update it with your path where the repo is cloned, however, the path should not contain the repo name, for example, the absolute path in my machine is
>/Users/emiliano.lara/Documents/Personal/Globant/globant_challenge_emiliano_lara
>
So the var `USER_PATH` will be like this:
>USER_PATH = "/Users/emiliano.lara/Documents/Personal/Globant/"

I also attached the J connector for MySQL it is the jar file named `mysql-connector-j-8.0.32.jar`  

After that, the code should run!