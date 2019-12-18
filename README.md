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
