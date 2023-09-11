git clone https://github.com/big-data-europe/docker-hadoop

# Aqui levantamos hadoop
sudo docker compose up

# Luego copiamos las clases de java a la 
# carpeta temporal.
sudo docker cp WordCount.java namenode:/tmp/
sudo docker cp input.txt namenode:/tmp/

# Nos vamos al terminal de namenode
sudo docker exec -it namenode bash

# Seteamos HADOOP_CLASSPATH
export HADOOP_CLASSPATH=$(hadoop classpath)

# Ejecutamos codigo dentro de hadoop
hadoop fs -mkdir /Input
# Esto no es necesario
# hadoop fs -mkdir /Output
hadoop fs -put /tmp/WordCount.java /Input
hadoop fs -put /tmp/input.txt /Input

mkdir classes

javac -classpath $(hadoop classpath) -d ./classes/ ./tmp/WordCount.java

# Esto genera el .jar
jar -cvf WordCount.jar -C ./classes .

hadoop jar WordCount.jar WordCount /Input/input.txt /Output

hadoop job -list all

hadoop fs -cat /Output/*

hadoop fs -cat /Output/* > output.txt

sudo docker cp namenode:/output.txt output.txt