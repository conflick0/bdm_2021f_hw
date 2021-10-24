# hw1
## Usage
### Start hadoop service
```
cd /opt/hadoop
```
```
sbin/start-all.sh
```
### Start spark service
```
cd /opt/spark
```
```
sbin/start-all.sh
```
### Push data to HDFS
```
cd /opt/hadoop
```
```
bin/hadoop fs -put /home/bdm/Desktop/bdm_2021f_hw/hw1/household_power_consumption.txt /user/bdm
```
### Run program
```
bin/spark-submit --master local /home/bdm/Desktop/bdm_2021f_hw/hw1/main.py
```
## Result

![result](https://user-images.githubusercontent.com/39295341/138590312-3e01dd9a-c369-4a3e-b6c6-287ecfe4ea85.PNG)
