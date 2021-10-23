# hw1
## Start hadoop service
```
cd /opt/hadoop
```
```
sbin/start-all.sh
```
## Start spark service
```
cd /opt/spark
```
```
sbin/start-all.sh
```
## Push data to HDFS
```
cd /opt/hadoop
```
```
bin/hadoop fs -put /home/bdm/Desktop/bdm_2021f_hw/hw1/household_power_consumption.txt /user/bdm
```
## Run program
```
bin/spark-submit --master local /home/bdm/Desktop/bdm_2021f_hw/hw1/main.py
```
