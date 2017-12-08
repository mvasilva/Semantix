# Spark - Resolução


### 1. Número de hosts únicos
txt_log = sc.textFile("hdfs:///user/cloudera/semantix/access_log_Jul95,hdfs:///user/cloudera/semantix/access_log_Aug95")

hosts = txt_log.map(lambda cols: cols.split(" - - ")).map(lambda cols: (cols[0],1)).reduceByKey(lambda a,b: a + b) 

hosts.keys().count()
#137979
###


### 2. Total de errors 404
step1 = txt_log.map(lambda cols: cols.split('] "'))

step2 = step1.map(lambda cols: cols[1] if(cols is not None and len(cols) > 1) else None)

step3 = step2.map(lambda cols: cols.split('" ') if(cols is not None and len(cols.split('" ')) > 1) else None)

rtnHttp_RtnBytes = step3.map(lambda cols: cols[1].split(' ') if(cols is not None and len(cols[1].split(' ')) > 1) else None)

step5 = rtnHttp_RtnBytes.map(lambda cols: (cols[0] if(cols is not None) else "-999",1)).reduceByKey(lambda a,b: a + b)

step5.collect()
#Foram 20873 retornos 404
#[('-999', 31), (u'304', 266773), (u'403', 224), (u'200', 3100521), (u'302', 73070), (u'500', 65), (u'501', 41), (u'404', 20873), (u'400', 15)]
###


### 3. Os 5 URLs que mais causaram erro 404
urls404 = step3.filter(lambda cols: (cols[1].split(' ')[0] if(cols is not None and len(cols[1].split(' ')) > 1) else None) == "404")

urls404Quebrado = urls404.map(lambda cols: cols[0].split(' '))

urls404ChaveValor = urls404Quebrado.map(lambda cols: (cols[1],1))

urls404Contagem = urls404ChaveValor.reduceByKey(lambda a, b: a + b)

urls404Contagem.takeOrdered(5, key = lambda r: -r[1])
#[(u'/pub/winvn/readme.txt', 2004), (u'/pub/winvn/release.txt', 1732), (u'/shuttle/missions/STS-69/mission-STS-69.html', 683), (u'/shuttle/missions/sts-68/ksc-upclose.gif', 428), (u'/history/apollo/a-001/a-001-patch-small.gif', 384)]
###

### 4. Quantidade de erros 404 por dia.
urls404ComTimestamp = step1.filter(lambda cols: (cols[1].split('" ')[1].split(' ')[0] if(cols is not None and len(cols) > 1 and cols[1] is not None and len(cols[1].split('" ')) > 1) and cols[1].split('" ')[1] is not None and cols[1].split('" ')[1].split(" ") is not None and len(cols[1].split('" ')[1].split(" ")) > 1 else None) == "404")

urls404ComTimestampQuebra = urls404ComTimestamp.map(lambda cols: cols[0].split(" - - ["))

urls404ComTimestampQuebraZona = urls404ComTimestampQuebra.map(lambda cols: cols[1].split(" "))

from datetime import datetime

urls404ComTimestampContagem = urls404ComTimestampQuebraZona.map(lambda cols: (datetime.strptime(cols[0], "%d/%b/%Y:%H:%M:%S").date(),1))

urls404ComTimestampContagemConsolidado =  urls404ComTimestampContagem.reduceByKey(lambda a, b: a + b)

urls404ComTimestampContagemTotal = urls404ComTimestampContagemConsolidado.values().reduce(lambda a,b:a+b)

urls404ComTimestampContagemTotal / urls404ComTimestampContagemConsolidado.keys().count()
#Em media são 359 erros 404 por dia
###

### 5. O total de bytes retornados
rtnBytes = rtnHttp_RtnBytes.map(lambda cols: int(cols[1] if(cols is not None and cols[1] is not None and cols[1] != '-') else 0) ).reduce(lambda a,b:a+b)

print(rtnBytes)
#Foram 65524307881 de bytes retornados
###




