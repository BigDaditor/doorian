from pywebhdfs.webhdfs import PyWebHdfsClient
import time


def getClient(url, port, user):
    client = PyWebHdfsClient(host=url, port=port, user_name=user)
    return client


def createFile(client, filename):
    try:
        client.get_file_dir_status('data/' + filename)
        return True
    except:
        print('Create File')
        client.create_file('data/' + filename, '')
        return False


def saveHdfs(client, filename):
    file = open('data/' + filename, 'a+')
    lines = file.readlines()
    for line in lines:
        client.append_file('data/' + filename, line)
