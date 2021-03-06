# Task
This task related to as data engineer. 

# Prerequisite
1. linux environment
2. python3+
3. postgresql
4. mongodb


## run
1. clean the json file (complex.json and tower.json)
```
grep -v '^/' complex.json > new_complex.json
grep -v '^/' tower.json > new_tower.json
```
2. create a database in mongodb and restore that file
3. create database in postgresql with the table that mentioned already
* make sure create user with encrypted password. the user and password that define in script is ```stock``` and ```stock```
* grant all privileges of database to that credentials
4. activate environment
```
source env/bin/activate
```
5. install all dependencies
```
pip install -r requirements.txt
```
6. run script process.py and make sure the version is python3+
```
python process.y
```

# Result
1. the data will be stored in database both mongodb and postgresql
2. it can be seen also as CSV file


# Data Architecture
![](data_architecture.jpg)
* the data from other source need to be ingested first through either API, GRPC or GraphQL, depends what interface used
* the data will be prepared to make sure the data exists and suitable with the requirement
* Process and transform the data will be required to filter what data that need to displayed and stored in analytical database
* Define what structure of data that need to stored. Define relation of table if necessay.
* create interface (API) in accessing data through analytical database
