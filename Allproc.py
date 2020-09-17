import requests
import shutil
import zipfile
import pandas as pd
import os
from multiprocessing.pool import ThreadPool
from tqdm import tqdm

 
def download_url(url):
  print("downloading:  ",url +'\n')
  # assumes that the last segment after the / represents the file name
  # if url is abc/xyz/file.txt, the file name will be file.txt
  file_name_start_pos = url.rfind("/") + 1
  file_name = url[file_name_start_pos:]
 
  r = requests.get(url, stream=True)
  
 
  if r.status_code == requests.codes.ok:
    with open(file_name, 'wb') as f:
      for data in r:
        f.write(data)
  return url
  
urls = ["https://static.stooq.com/db/h/d_world_txt.zip",
        "https://static.stooq.com/db/h/d_us_txt.zip",
        "https://static.stooq.com/db/h/h_world_txt.zip",
        "https://static.stooq.com/db/h/h_us_txt.zip"]
 
# Run 5 multiple threads. Each call will take the next element in urls list
results = ThreadPool(4).imap_unordered(download_url, urls)
for r in results:
    print(r)
    
print("DESCARGAS FINALIZADAS")

########################### FIN DE DESCARGAS ###############################

print("VACIANDO CARPETAS")

path = "/USAdb/d_us_txt/data"
shutil.rmtree(path)

path1= "/USAdb/d_world_txt/data"
shutil.rmtree(path1)

path2= "/USAdb/Hs/h_us_txt/data"
shutil.rmtree(path2)

path3= "/USAdb/Hs/h_world_txt/data"
shutil.rmtree(path3)

print("VACIADO FINALIZADO")

################################# FIN BORRAR ARCHIVOS ANTERIORES #######################################
print("INICIANDO ZIPS \n")

print("h_world_txt.zip \n")
fh = open('h_world_txt.zip', 'rb')
z = zipfile.ZipFile(fh)
for name in tqdm(z.namelist()):
    outpath = r'C:\USAdb\Hs\h_world_txt'
    z.extract(name, outpath)
fh.close()


print("d_world_txt.zip \n")
fh = open('d_world_txt.zip', 'rb')
z = zipfile.ZipFile(fh)
for name in tqdm(z.namelist()):
    outpath = r'C:\USAdb\d_world_txt'
    z.extract(name, outpath)
fh.close()

print("Borrando prn.txt /n")

zin = zipfile.ZipFile ('d_us_txt.zip', 'r')
zout = zipfile.ZipFile ('d_us1_txt.zip', 'w')
for item in tqdm(zin.infolist()):
    buffer = zin.read(item.filename)
    if (item.filename != "data/daily/us/nyse etfs/prn.us.txt"):
          zout.writestr(item, buffer)
zout.close()
zin.close()

print("d_us1_txt.zip \n")
fh = open('d_us1_txt.zip', 'rb')
z = zipfile.ZipFile(fh)
for name in z.namelist():   
        outpath = r'C:\USAdb\d_us_txt'
        z.extract(name, outpath)
fh.close()

print("Borrando prn.txt \n")

zin = zipfile.ZipFile ('h_us_txt.zip', 'r')
zout = zipfile.ZipFile ('h_us1_txt.zip', 'w')
for item in tqdm(zin.infolist()):
    buffer = zin.read(item.filename)
    if (item.filename != "data/hourly/us/nyse etfs/prn.us.txt"):
          zout.writestr(item, buffer)
zout.close()
zin.close()

print("h_us1_txt.zip \n")
fh = open('h_us1_txt.zip', 'rb')
z = zipfile.ZipFile(fh)
for name in tqdm(z.namelist()):   
        outpath = r'C:\USAdb\Hs\h_us_txt'
        z.extract(name, outpath)
fh.close()

###################################################
############# MODIFICANDO TXT 60 MIN ##############
###################################################

directory = r'C:\USAdb\Hs'

print("modificando txt 60 min \n")
for root, direct, files in os.walk(directory):
    for file in tqdm(files):   
        #filename = os.path.join(root, files)
        if file.endswith(".txt") and os.stat(root + '/' + file).st_size > 0:
            df=pd.read_csv(root + '/' + file, sep=',')
            filas=len(df.index)
            #print("Filas: ",filas)
            if filas > 10000:
                df.drop(df.index[0:filas-10000],inplace=True)
                s= pd.date_range('1990-01-01',periods=filas)
                df['<DATE>']=pd.DataFrame(s, columns=['<DATE>'])
                df.to_csv(root + '/' + os.path.splitext(file)[0] + '60' + '.txt',date_format='%Y%m%d', float_format='%.3f', sep=',',index=False)
                os.remove(root + '/' + file)
            else:
                s= pd.date_range('1990-01-01',periods=filas)
                df['<DATE>']=pd.DataFrame(s, columns=['<DATE>'])
                df.to_csv(root + '/' + os.path.splitext(file)[0] + '60' + '.txt',date_format='%Y%m%d', float_format='%.3f', sep=',',index=False)
                os.remove(root + '/' + file)


# ############# COPIAR ARCHIVOS################################
print("copiando archivos a nyse stocks \n")


Dest = r'C:\USAdb\d_us_txt\data\daily\us\nyse stocks'
# First, create a list and populate it with the files
# you want to find (1 file per row in myfiles.txt)
filesToFind = []
with open('myfiles.txt', "r") as fh:
    for row in fh:
        #print(row.strip())
        filesToFind.append(row.strip())
    print(filesToFind)
    print(len(filesToFind))
#Then we recursively traverse through each folder
#and match each file against our list of files to find.
for root, dirs, files in os.walk(r'C:\USAdb'):
    for _file in files:
        if _file in filesToFind:
            #If we find it, notify us about it and copy it it to C:\NewPath\
            print ('Found file in: '+ str(root) + '  ' +_file)
            shutil.copy(os.path.abspath(root + '/' + _file), Dest)
            filesToFind.remove(_file)
            
            
            
##################### AJUSTAR ARCHIVOS COPIADOS A nyse stocks POR Q LINEAS ##########

print("ajustando largo de archivos")

directory = r'C:\USAdb\d_us_txt\data\daily\us\nyse stocks'


for file in os.listdir(directory):
      filename = os.fsdecode(file)
      if filename.endswith(".txt"):
        df=pd.read_csv(directory+'/'+filename, sep=',')
        filas=len(df.index)
        if filas > 10000:
            df.drop(df.index[0:filas-10000],inplace=True)
            df.to_csv(directory+'/'+filename, float_format='%.3f', sep=',',index=False)
        filas=len(df.index)
        print("Filas: ",filas)
         
        
print("################## FIN ####################")

###################################FIN##################################################