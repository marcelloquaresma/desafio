from operator import add

#log_aug95 = sc.textFile("/FileStore/tables/access_log_Aug95_menor")
aug95 = sc.textFile("/FileStore/tables/access_log_Aug95")
log_aug95 = aug95.cache()

#1. Número de hosts únicos.
hosts = log_aug95.map(lambda s: s.split(" ")[0]).distinct().count()

#2. O total de erros 404.
qtde_error_404 = log_aug95.filter(lambda line: line.split(' ')[-2] == '404')

#agrupa as urls com erro 404
urls_404_agg = qtde_error_404.map(lambda line: line.split('"')[1].split(' ')[1])

#3. Os 5 URLs que mais causaram erro 404.
urls_mais_erros = qtde_error_404.map(lambda line: line.split('"')[1].split(' ')[1])
urls_404_agg = urls_mais_erros.map(lambda x: (x, 1)).reduceByKey(add)
top5 = urls_404_agg.sortBy(lambda s: -s[1]).take(5)

#4. Quantidade de erros 404 por dia
erros_dia = qtde_error_404.map(lambda x: x.split('[')[1].split(':')[0])
qtde_erros_dia = erros_dia.map(lambda x: (x, 1)).reduceByKey(add).collect()

#5. O total de bytes retornados.
def sum_total_tytes(rdd):
    def qtde_bytes(reg):
        try:
            getbytes = int(reg.split(" ")[-1])
            if getbytes < 0:
                raise ValueError()
            return getbytes
        except:
            return 0
        
    getbytes = rdd.map(qtde_bytes).reduce(add)
    return getbytes
  
print("------")
print("1. Número de hosts únicos: ", hosts)

print("------")
print("2. O total de erros 404: ", qtde_error_404.count())

print("------")
print("3. Os 5 URLs que mais causaram erro 404: ")
for urls, qtde_erros in top5:
  print(urls, qtde_erros)
  
print("------")  
print("4. Quantidade de erros 404 por dia:")
for dia, qtde in qtde_erros_dia:
  print(dia, qtde)
  
print("------")  
print("5. O total de bytes retornados:", sum_total_tytes(log_aug95))

#Resultados

#------
#1. Número de hosts únicos:  75060
#------
#2. O total de erros 404:  10056
#------
#3. Os 5 URLs que mais causaram erro 404: 
#/pub/winvn/readme.txt 1337
#/pub/winvn/release.txt 1185
#/shuttle/missions/STS-69/mission-STS-69.html 683
#/images/nasa-logo.gif 319
#/shuttle/missions/sts-68/ksc-upclose.gif 253
#------
#4. Quantidade de erros 404 por dia:
#05/Aug/1995 236
#10/Aug/1995 315
#11/Aug/1995 263
#12/Aug/1995 196
#13/Aug/1995 216
#17/Aug/1995 271
#18/Aug/1995 256
#24/Aug/1995 420
#26/Aug/1995 366
#28/Aug/1995 410
#30/Aug/1995 571
#03/Aug/1995 304
#04/Aug/1995 346
#06/Aug/1995 373
#07/Aug/1995 537
#08/Aug/1995 391
#14/Aug/1995 287
#16/Aug/1995 259
#20/Aug/1995 312
#29/Aug/1995 420
#01/Aug/1995 243
#09/Aug/1995 279
#15/Aug/1995 327
#19/Aug/1995 209
#21/Aug/1995 305
#22/Aug/1995 288
#23/Aug/1995 345
#25/Aug/1995 415
#27/Aug/1995 370
#31/Aug/1995 526
#------
#5. O total de bytes retornados: 26828341424
