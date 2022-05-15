# Set up StackOverflow DB
(https://sqlblog.org/2020/03/15/attaching-restoring-databases-inside-a-container)

Get the SO 2013 DB here:

`curl -L -o "so.7z" "https://downloads.brentozar.com/StackOverflow2013_201809117.7z"`

`mkdir -p volumes/data`
then unzip the 7z file into that volumes/data folder

Run the run.sh

```
CREATE DATABASE StackOverflow 
ON 
  (name = so1,   filename = N'/var/opt/mssql/data/StackOverflow2013_1.mdf'),
  (name = so2,   filename = N'/var/opt/mssql/data/StackOverflow2013_2.ndf'),
  (name = so3,   filename = N'/var/opt/mssql/data/StackOverflow2013_3.ndf'),
  (name = so4,   filename = N'/var/opt/mssql/data/StackOverflow2013_4.ndf')
LOG ON
  (name = solog, filename = N'/var/opt/mssql/data/StackOverflow2013_log.ldf')
FOR ATTACH;
```
