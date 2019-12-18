1- Qual o objetivo do comando cache em Spark?

Ajuda na armazenagem e rápida reutilização das operações lazy (operações de transformação), fazendo assim com que ocorra uma melhor 
eficiência do código nos repetitivos processos entre os cenários de transformação e ação em datasets. 

Transformação = instruções e Ação = execução das instruções.

2- O mesmo código implementado em Spark é normalmente mais rápido que a implementação equivalente em
MapReduce. Por quê?

Mapreduce é executado (leitura/escrita) em disco e Spark (que também utiliza Mapreduce em sua "inteligência" funcional) é executado em 
memória fazendo as operações de reuso (cache) e podendo ser até cem vezes mais rápido que o MapReduce simplesmente. Cada job Mapreduce novo ao 
ser disparado, acionará uma nova jvm, já o Spark manterá uma jvm ativa em cada nó do cluster e dispara apenas uma nova execução.

3- Qual é a função do SparkContext?

Alocar recursos (memória, processador), criação de RDD, de broadcast (transmissão), acumuladores (variáveis de gravação) e disparar jobs.  

4- Explique com suas palavras o que é Resilient Distributed Datasets (RDD).

Resilient Distributed Datasets. Tolerante à falhas, podendo reprocessar dados perdidos por falhas dos nós do cluster (por isso é 
resiliente), dividido em partições de diferentes nós do cluster (por isso é distribuído), atuando sobre conjuntos de dados (datasets). RDDs são 
imutáveis. Em caso de necessidade de modificação, ocorre em transformações que geram novos RDDs. Permite execução paralela e é categorizado por 
tipos (integer, float, string, etc).

5- GroupByKey é menos eficiente que reduceByKey em grandes dataset. Por quê?

GroupByKey não utiliza cálculo parcial de resultados aumentando consumo de memória e gerando necessidade de escrita de dados em disco, que 
torna o processo (leitura nesse caso) muito mais lento em especial em um grande dataset. Já reduceByKey realiza o processo em todos os elementos 
de mesma chave em cada partição para obter um resultado parcial antes de passar os dados para os executores que vão calcular o result, gerando 
assim menor consumo de memória para os dados transferidos.

6- Explique o que o código Scala abaixo faz.

val textFile = sc.textFile("hdfs://...")
val counts = textFile.flatMap(line => line.split(" "))
.map(word => (word, 1))
.reduceByKey(_ + _)
counts.saveAsTextFile("hdfs://...")

Linha 1- Leitura de um arquivo texto no path do hdfs apontado entre parênteses e aspas duplas;
Linha 2- Quebra/divisão de linha (split) em sequencias de palavras e cada sequência é transformada em uma coleção de palavras;
Linha 3- Cada palavra é transformada num mapping chave-valor e a chave é a palavra mesmo com valor igual a 1;
Linha 4- Agrupamento dos valores pela chave concatenando com o sinal de +;
Linha 5- O resultado final é salvo em um novo arquivo texto no path do hdfs apontado entre parênteses e aspas duplas;




##################################################################################################################################

#Código Spark

from pyspark import SparkConf,SparkContext
from operator import add

conf = (SparkConf()
         .setMaster("local")
         .setAppName("SPark")
         .set("spark.executor.memory","2g"))
sc = SparkContext(conf = conf)
july = sc.textFile('access_log_Jul95')
july = july.cache()
august = sc.textFile('access_log_Aug95')
august = august.cache()

#qtd hosts distintos

july_count = july.flatMap(lambda line:line.split(' ')[0]).distinct().count()
august_count = august.flatMap(lambda line:line.split(' ')[0]).distinct().count()
print('Hosts Julho: %s' % july_count)
print('Hosts Agosto: %s' % august_count)

#qtd 404

def response_code_404(line):
    try:
        code = line.split(' ')[-2]
        if code == '404':
            return True
    except:
        pass
    return False
july_404 = july.filter(response_code_404).cache()
august_404 = august.filter(lambda line:line.split(' ')[-2] == '404').cache()
print('404 Julho: %s' % july_404.count())
print('404 Agosto: %s' % august_404.count())

# top 5 endpoint - 404

def top5_endpoints(rdd):
    endpoints = rdd.map(lambda line:line.split('"')[1].split(' ')[1])
    counts = endpoints.map(lambda endpoint:(endpoint,1)).reduceByKey(add)
    top = counts.sortBy(lambda pair: -pair[1]).take(5)
    print('\nTop 5 Endpoint - 404:')
    for endpoint,count in top:
        print(endpoint,count)   
    return top
top5_endpoints(july_404)
top5_endpoints(august_404)

# 404 diário

def daily_count(rdd):
    days = rdd.map(lambda line:line.split('[')[1].split(':')[0])
    counts = days.map(lambda day:(day,1)).reduceByKey(add).collect()
    print('\n404 - dia:')
    for day,count in counts:
        print(day,count)
    return counts
daily_count(july_404)
daily_count(august_404)

# Total bytes

def accumulated_byte_count(rdd):
    def byte_count(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                raise ValueError()
            return count
        except:
            return 0
    count = rdd.map(byte_count).reduce(add)
    return count
print('Bytes Julho: %s' % accumulated_byte_count(july))
print('Bytes Agosto: %s' % accumulated_byte_count(august))

sc.stop()
