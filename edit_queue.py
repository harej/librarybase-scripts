import imp
import importlib
import queue
import threading
import urllib.parse
from time import sleep
from wikidataintegrator import wdi_core
from wikidataintegrator.wdi_config import config
from site_credentials import *

class EditQueue:
    def __init__(self, source, url_pattern, write_thread_count=4, append_value=[], good_refs=[], edit_summary='', alt_account=False):
        self.source = source
        self.url_pattern = url_pattern
        self.append_value = append_value
        self.good_refs = good_refs
        self.edit_summary = edit_summary

        global site_username
        global site_password

        if alt_account is True:
            site_username = site_username_2
            site_password = site_password_2

        self.integrator = []
        for n in range(0, write_thread_count):
            self.integrator.append({})
            self.integrator[n]['parent'] = imp.load_module('integrator' + str(n), *imp.find_module('wikidataintegrator'))
            self.integrator[n]['login']  = importlib.import_module(self.integrator[n]['parent'].__name__ + '.wdi_login').\
                                                WDLogin(user=site_username, pwd=site_password)
            self.integrator[n]['core']   = importlib.import_module(self.integrator[n]['parent'].__name__ + '.wdi_core')

        self.editqueue = queue.Queue(maxsize=0)
        self.event = threading.Event()
        self.editors = [threading.Thread(target=self.do_edits, kwargs={'n': n, 'event': self.event}) \
                        for n in range(0, write_thread_count)]

        for editor in self.editors:
            editor.start()

    def task_processor(self, task, n):
        ref = [[wdi_core.WDItemID(
                    value=self.source,
                    prop_nr='P248',
                    is_reference=True),
                wdi_core.WDUrl(
                     value=self.url_pattern + urllib.parse.quote_plus(str(task[1])),
                     prop_nr='P854',
                     is_reference=True),
                wdi_core.WDTime(
                     task[2],
                     prop_nr='P813',
                     is_reference=True)]]

        data = []
        for cited_item in task[3]:
            data.append(wdi_core.WDItemID(
                            value='Q' + str(cited_item),
                            prop_nr='P2860',
                            references=ref))

        itemengine = self.integrator[n]['core'].WDItemEngine(
                        wd_item_id='Q' + str(task[0]),
                        data=data,
                        append_value=self.append_value,
                        good_refs=self.good_refs,
                        keep_good_ref_statements=True)
        print(itemengine.write(self.integrator[n]['login'], edit_summary=self.edit_summary))

    def do_edits(self, n, event):
        while True:
            try:
                task = self.editqueue.get(timeout=1)
            except queue.Empty:
                if self.event.isSet():
                    break
                else:
                    continue
            try:
                self.task_processor(task, n)
                sleep(0.25)
            except Exception as e:
                print('Exception when trying to edit ' + str(task[0]) + '; skipping')
                print(e)
            self.editqueue.task_done()

    def post(self, relevant_item, relevant_external_id, retrieve_date, cites):
        while self.editqueue.qsize() > 1000:
            sleep(0.25)
        # Shedding some weight to take up less space in RAM.
        relevant_item = int(relevant_item.replace('Q', ''))
        cites = tuple([int(x.replace('Q', '')) for x in cites])
        self.editqueue.put((relevant_item, relevant_external_id, retrieve_date, cites))

    def done(self):
        self.event.set()
