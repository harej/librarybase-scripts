import imp
import importlib
import queue
import threading
import urllib.parse
from wikidataintegrator import wdi_core
from site_credentials import *

class EditQueue:
    def __init__(self, source, url_pattern, write_thread_count=4, append_value=[], good_refs=[], edit_summary=''):
        self.source = source
        self.url_pattern = url_pattern
        self.append_value = append_value
        self.good_refs = good_refs
        self.edit_summary = edit_summary

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

    def get_reference(self, retrieve_date, relevant_external_id):
        return [[wdi_core.WDItemID(
                    value=self.source,
                    prop_nr='P248',
                    is_reference=True),
                 wdi_core.WDUrl(
                     value=self.url_pattern + urllib.parse.quote_plus(relevant_external_id),
                     prop_nr='P854',
                     is_reference=True),
                 wdi_core.WDTime(
                     retrieve_date,
                     prop_nr='P813',
                     is_reference=True)]]

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
                data = []
                for cited_item in task[3]:
                    data.append(wdi_core.WDItemID(
                                value=cited_item,
                                prop_nr='P2860',
                                references=self.get_reference(task[2], task[1])))

                itemengine = self.integrator[n]['core'].WDItemEngine(
                                wd_item_id=task[0],
                                data=data,
                                append_value=self.append_value,
                                good_refs=self.good_refs,
                                keep_good_ref_statements=True)
                print(itemengine.write(self.integrator[n]['login'], edit_summary=self.edit_summary))
            except Exception as e:
                print('Exception when trying to edit ' + task[0] + '; skipping')
                print(e)
            self.editqueue.task_done()

    def post(self, relevant_item, relevant_external_id, retrieve_date, cites):
        self.editqueue.put((relevant_item, relevant_external_id, retrieve_date, cites))

    def done(self):
        self.event.set()
