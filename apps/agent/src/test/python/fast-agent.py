#! encoding=utf-8

import requests
import sys
import codecs
import gzip
import json
import hashlib
import time
import logging

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(asctime)s - %(threadName)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)

logger = logging.getLogger('')
logger.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)

server_url = 'http://172.18.109.137:9999/cluster'

# external
external_base_url = server_url + '/external'


def register_external_batch(token):
    registry_url = external_base_url + '/batch/registry'
    resp = requests.post(url=registry_url,
                         data={
                             'token': token
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        logger.info(resp.text)
        return None


def get_external_batch_status(token, ext_batch_id):
    get_url = external_base_url + '/batch/status'
    resp = requests.post(url=get_url,
                         data={
                             'token': token,
                             'external_id': ext_batch_id
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def terminate_external_batch(token, ext_batch_id):
    termination_url = external_base_url + '/batch/close'
    resp = requests.post(url=termination_url,
                         data={
                             'token': token,
                             'external_id': ext_batch_id
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        logger.info(resp.text)
        return None


# internal
def registry_fast_agent(name, ip, mac, identity, cpu_core, ram, version):
    name = 'jdjd-jp-007'
    ip = '172.105.193.227'
    mac = 'f2:3c:91:77:b7:99'
    identity = name + '/' + ip + '/' + mac
    registry_url = server_url + '/server'
    cpu_core = 1
    ram = 4096000000
    version = 'v1.26.0'

    resp = requests.post(url=registry_url,
                         data={
                             'identity': identity,
                             'cpu_core': cpu_core,
                             'ram': ram,
                             'version': version
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def get_task(token):
    get_task_url = server_url + '/task/get'
    resp = requests.post(url=get_task_url,
                         data={
                             'token': token
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def submit_task(token, batch_id):
    # set up task
    page_url = 'https://www.facebook.com/HuaweimobileMY'
    # read file content
    with codecs.open('page.html', 'r', 'utf-8') as fp:
        page_content = fp.read()
        fp.close()
    # set a JSON page
    page = {
        'headers': {},
        'status_code': 200,
        'final_url': 'https://www.facebook.com/AppleInc.HD/',
        'success': True,
        'time_elapsed': 1000,
        'content': page_content,
        'url': page_url,
        'parser': 'page',
        'source': 'facebook',
        'page_type': 'post-1',
        'external_id': batch_id
    }
    # set page in tasks list
    tasks = [page]
    gz_filename = 'file_examples.gz'
    with gzip.open(gz_filename, 'wb') as gzfp:
        gzfp.write(json.dumps(tasks))
        gzfp.close()

    with open(gz_filename, 'rb') as fp:
        gzfile_content = fp.read()
        md5 = hashlib.md5(gzfile_content).hexdigest()
        fp.close()

    submit_task_url = server_url + '/task/submit'

    resp = requests.post(url=submit_task_url,
                         data={
                             'token': token,
                             'md5_checksum': md5,
                             'inquire': False},
                         files={
                             'file': gzfile_content
                         })
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def get_user_agent():
    fetch_url = server_url + '/task/ua'
    resp = requests.get(url=fetch_url)

    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def external_main():
    # registry a batch
    token = 123456
    batch_id = 0
    while True:
        result = register_external_batch(token)
        if result is None:
            # crawler has a problem or illegal arguments
            time.sleep(60)
            continue
        logger.info(result)
        status = result['status']
        if status == 'ERR':
            mesg = result['mesg']
            if mesg == 'ERR_BATCH_INIT':
                # network or MySQL down, notify 6crawler developers, then sleep 1 minutes
                time.sleep(60)
                continue
            elif mesg == 'ERR_DATA_SAVER_CREATE':
                # network or MySQL down, notify 6crawler developers, then sleep 1 minutes, close current batch
                batch_id = result['batch_id']
                time.sleep(60)
                terminate_external_batch(token, batch_id)
                continue
            elif mesg == 'ERR_BATCH_RUN':
                # network or MySQL down, notify 6crawler developers, then sleep 1 minutes, close current batch
                batch_id = result['batch_id']
                time.sleep(60)
                terminate_external_batch(token, batch_id)
                continue
        elif status == 'SUCCESS':
            batch_id = result['batch_id']
            logger.info(str(batch_id))
            break

    submit_task(token, batch_id)

    # get a batch status
    result = get_external_batch_status(token, batch_id)
    if result:
        status = result['status']
        if status == 'ERR':
            mesg = result['mesg']
            if mesg == 'ERR_BATCH_NOT_EXIST':
                # batch not existed, re-registry
                batch_id = register_external_batch(token)['batch_id']
            elif mesg == 'ERR_BATCH_STATUS':
                # unexpect error, maybe MySQL shutdown
                time.sleep(60)
        elif status == 'SUCCESS':
            batch = result['batch']
            batch_id = batch['batch_id']
            batch_count = batch['count']
            # "0": RUNNING, "1": DONE, "2": INITING, "-2": DISABLED, "3": COLLECTED
            batch_status = batch['status']

            logger.info("batch id: " + str(batch_id))
            logger.info("batch count: " + str(batch_count))
            logger.info("batch status: " + batch_status)

    # terminate a batch status
    result = terminate_external_batch(token, batch_id)
    logger.info(result)
    if result:
        status = result['status']
        if status == 'ERR':
            mesg = result['mesg']
            if mesg == 'ERR_BATCH_TERMINATE':
                # error for terminate, double check batch id, and notify 6crawler developer
                time.sleep(60)
        elif status == 'SUCCESS':
            logger.info("good end")


external_main()








