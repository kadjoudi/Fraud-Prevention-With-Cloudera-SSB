
## Create "transaction" Table In Apache Kudu

create TABLE transactions
(
ts string,
acc_id string,
transaction_id string,
amount bigint,
lat double,
lon double,
PRIMARY KEY (ts, acc_id)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');


## Create "customer_temp"  Table In Apache Impala
CREATE external TABLE customer_temp
(
acc_id string,
f_name string,
l_name string,
email string,
gendre string,
phone string,
card string)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE;

## Load CSV Data from S3 To Impala

LOAD DATA INPATH 's3a://kdj-demo/my-data/customer-data.csv' INTO TABLE default.customer_temp;


##Create "customers"  Table In Apache Kudu
CREATE TABLE customers
PRIMARY KEY (acc_id)
PARTITION BY HASH PARTITIONS 16

STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '3')
AS select  acc_id,f_name,l_name,email,gendre,phone,card  from customer_temp;

// Drop temporary "customer_temp" table
DROP TABLE customer_temp ;

##Create "fraudulent_txn"  Table In Apache Kudu

create TABLE fraudulent_txn
(
event_time string,
acc_id string,
transaction_id string,
f_name string,
l_name string,
email string,
gendre string,
phone string,
card string,
lat double,
lon double,
amount bigint,
PRIMARY KEY (event_time, acc_id)
)
PARTITION BY HASH PARTITIONS 16
STORED AS KUDU
TBLPROPERTIES ('kudu.num_tablet_replicas' = '3');
