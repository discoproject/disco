from datetime import datetime

from disco.events import Event, Message, AnnouncePID, DataUnavailable, Output, EventRecord
from disco.error import JobError
from disco.test import TestCase, TestJob

class EventFormatTestCase(TestCase):
    def test_event(self):
        event_record = EventRecord(str(Event('message', tags=['mem', 'cpu'])))
        self.assertEquals('EV', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals(['mem', 'cpu'], event_record.tags)
        self.assertEquals('message', event_record.payload)

    def test_message(self):
        event_record = EventRecord(str(Message('message\n $#!%')))
        self.assertEquals('MSG', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals([], event_record.tags)
        self.assertEquals('message\n $#!%', event_record.payload)

    def test_announce_pid(self):
        event_record = EventRecord(str(AnnouncePID('666')))
        self.assertEquals('PID', event_record.type)
        self.assertEquals('666', event_record.payload)

    def test_data_unavailable(self):
        event_record = EventRecord(str(DataUnavailable('http://localhost:8989/path')))
        self.assertEquals('DAT', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals([], event_record.tags)
        self.assertEquals('http://localhost:8989/path', event_record.payload)

    def test_output_url(self):
        timestamp = datetime.now().strftime(Event.timestamp_format)
        time = datetime.strptime(timestamp, Event.timestamp_format)
        event_record = EventRecord(str(Output(['disco://master'])))
        self.assertEquals('OUT', event_record.type)
        self.assert_(event_record.time >= time)
        self.assertEquals([], event_record.tags)
        self.assertEquals(['disco://master'], event_record.payload)

    def test_bad_tags(self):
        self.assertRaises(TypeError, EventRecord(str(Event('', tags=['bad-']))))
        self.assertRaises(TypeError, EventRecord(str(Event('', tags=['bad ']))))

class JobEventTestCase(TestCase):
    def serve(self, path):
        return 'data\n' * 5

    def new_job(self, **kwargs):
        return TestJob().run(input=self.test_server.urls([1]), **kwargs)

    def test_single_line_error(self):
        def map(e, params):
            import sys, disco.json
            msg = disco.json.dumps("Single line error!")
            sys.stderr.write('ERR %d %s\n' % (len(msg), msg))
        self.job = self.new_job(map=map)
        self.assertRaises(JobError, self.job.wait)

    def test_single_line_message(self):
        def map(e, params):
            import sys, disco.json
            msg = disco.json.dumps("Single line message")
            sys.stderr.write('MSG %d %s\n' % (len(msg), msg))
            return []
        self.job = self.new_job(map=map)
        self.assertResults(self.job, [])

    def test_binary_message(self):
        def map(e, params):
            print '\x00\x001\xc9D\x8b-\xa0\x99 \x00\xba\xc0\xe5`\x00H\x89'
            '\xdeH\x8b=\x81\x99 \x00\xe8\x04\xeb\xff\xff\x85\xc0\x0f\x88l'
            return []
        self.job = self.new_job(map=map)
        self.assertResults(self.job, [])

    def test_utf8_message(self):
        def has_valid_event(events):
            msg = u'\xc4\xe4rett\xf6myys'
            for n, e in events:
                if msg in e[2]:
                    return True
        def map(e, params):
            print u'\xc4\xe4rett\xf6myys'
            return []
        self.job = self.new_job(map=map)
        self.assertResults(self.job, [])
        self.assertTrue(has_valid_event(self.job.events()))

    def test_non_utf8_message(self):
        def map(e, params):
            print u'\xc4\xe4rett\xf6myys'.encode('latin-1')
            return []
        self.job = self.new_job(map=map)
        self.assertResults(self.job, [])
