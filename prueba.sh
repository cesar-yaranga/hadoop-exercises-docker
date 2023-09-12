# git clone https://github.com/big-data-europe/docker-hadoop
# Aqui levantamos hadoop
sudo docker compose up

# Luego copiamos las clases de java a la 
# carpeta temporal.
sudo docker cp SalesCountry.java namenode:/tmp/
sudo docker cp ../SalesJan2009.csv namenode:/tmp/

# Nos vamos al terminal de namenode
sudo docker exec -it namenode bash

# Seteamos HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$(hadoop classpath)

# Ejecutamos codigo dentro de hadoop
hadoop fs -mkdir /Input
hadoop fs -put /tmp/SalesJan2009.csv /Input

mkdir classes

javac -classpath $(hadoop classpath) -d ./classes/ ./tmp/SalesCountry.java

# Esto genera el .jar
jar -cvf SalesCountry.jar -C ./classes .

hadoop jar SalesCountry.jar SalesCountry /Input/SalesJan2009.csv /Output

# hadoop job -list all

# hadoop fs -cat /Output/*

hadoop fs -cat /Output/* > output.txt

sudo docker cp namenode:/output.txt output.txt