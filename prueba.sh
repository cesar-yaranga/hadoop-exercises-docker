# git clone https://github.com/big-data-europe/docker-hadoop
# Aqui levantamos hadoop
sudo docker compose up

# Luego copiamos las clases de java a la 
# carpeta temporal.
sudo docker cp SalesCountryDriver.java namenode:/tmp/
sudo docker cp ../mi_archivo.csv namenode:/tmp/
sudo docker exec -it namenode bash

# Seteamos HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$(hadoop classpath)

# Ejecutamos codigo dentro de hadoop
hadoop fs -mkdir /Input
hadoop fs -put /tmp/mi_archivo.csv /Input

mkdir classes

javac -classpath $(hadoop classpath) -d ./classes/ ./tmp/SalesCountryDriver.java

# Esto genera el .jar
jar -cvf SalesCountryDriver.jar -C ./classes .

hadoop jar SalesCountryDriver.jar SalesCountryDriver /Input/mi_archivo.csv /Output

# hadoop job -list all

# hadoop fs -cat /Output/*

hadoop fs -cat /Output/* > output.txt

sudo docker cp namenode:/output.txt output.txt



sudo docker cp SalesCountryDriver.java namenode:/tmp/
sudo docker exec -it namenode bash
