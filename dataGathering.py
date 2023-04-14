import urllib3
import datetime

http = urllib3.PoolManager()
todaysDate = datetime.date.today().strftime("%m-%d-%Y-")

resp = http.request('GET' , 'http://psudataeng.com:8000/getBreadCrumbData')
print(resp.status)
# print(resp.data)

if(resp.status == 200):
    with open(todaysDate + 'TriMet.txt', 'wb') as writeOut:
        writeOut.write(resp.data)
    writeOut.close()