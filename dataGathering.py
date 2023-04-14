import urllib3
import datetime
http = urllib3.PoolManager()
todaysDate = datetime.date.today().strftime("%m-%d-%Y-")

print("Reaching out to TriMet data source .. ", end='', flush = True)

def requestTrimetData():
    return(http.request('GET' , 'http://psudataeng.com:8000/getBreadCrumbData'))
    # print(resp.status)
    # print(resp.data)

def TriMetToText(resp):
    if(resp.status == 200):
        print('Successful!')
        print("New file "+ todaysDate+"TriMet.txt created.")
        with open(todaysDate + 'TriMet.txt', 'wb') as writeOut:
            writeOut.write(resp.data)
        writeOut.close()
    else:
        print('Unsuccessful ..')
        print('Something went wrong, no new file created.')

resp = requestTrimetData()
TriMetToText(resp)
