from disco.test import DiscoJobTestFixture, DiscoTestCase
from disco.events import Event, Message, AnnouncePID, DataUnavailable, OutputURL, EventRecord
from disco.error import JobError

from datetime import datetime
from binascii import hexlify

class EventFormatTestCase(DiscoTestCase):
    def test_event(self):
        event_record = EventRecord(str(Event('message', tags=['mem', 'cpu'])))
        self.assertEquals('EV', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals(['mem', 'cpu'], event_record.tags)
        self.assertEquals('message', event_record.message)

    def test_message(self):
        event_record = EventRecord(str(Message('message\n $#!%')))
        self.assertEquals('MSG', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals([], event_record.tags)
        self.assertEquals('message\n $#!%', event_record.message)

    def test_announce_pid(self):
        event_record = EventRecord(str(AnnouncePID(666)))
        self.assertEquals('PID', event_record.type)
        self.assertEquals('666', event_record.message)

    def test_data_unavailable(self):
        event_record = EventRecord(str(DataUnavailable('http://localhost:8989/path')))
        self.assertEquals('DAT', event_record.type)
        self.assert_(event_record.time < datetime.now())
        self.assertEquals([], event_record.tags)
        self.assertEquals('http://localhost:8989/path', event_record.message)

    def test_output_url(self):
        timestamp = datetime.now().strftime(Event.timestamp_format)
        time = datetime.strptime(timestamp, Event.timestamp_format)
        event_record = EventRecord(str(OutputURL('disco://master')))
        self.assertEquals('OUT', event_record.type)
        self.assert_(event_record.time >= time)
        self.assertEquals([], event_record.tags)
        self.assertEquals('disco://master', event_record.message)

    def test_bad_tags(self):
        self.assertRaises(TypeError, EventRecord(str(Event('', tags=['bad-']))))
        self.assertRaises(TypeError, EventRecord(str(Event('', tags=['bad ']))))

class SingleLineMessageTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'data\n' * 10

    @staticmethod
    def map(e, params):
        import sys
        sys.stderr.write('**<MSG> Single line message\n')
        return []

    @property
    def answers(self):
        return []

class BinaryMessageTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'data\n' * 10

    @staticmethod
    def map(e, params):
        import sys
        l='\x00\x001\xc9D\x8b-\xa0\x99 \x00\xba\xc0\xe5`\x00H\x89\xdeH\x8b=\x81\x99 \x00\xe8\x04\xeb\xff\xff\x85\xc0\x0f\x88l\n'
        sys.stderr.write('**<MSG> Binary message ' + l)
        return []

    @property
    def answers(self):
        return []

class SingleLineErrorTestCase(SingleLineMessageTestCase):
    @staticmethod
    def map(e, params):
        import sys
        sys.stderr.write('**<ERR> Single line error!\n')
        return []

    def runTest(self):
        self.assertRaises(JobError, self.job.wait)

class UTF8MessageTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'data\n' * 10

    @staticmethod
    def map(e, params):
        import sys
        print u'\xc4\xe4rett\xf6myys'
        return []

    @property
    def answers(self):
        return []

    def has_valid_event(self, events):
        msg = u'\xc4\xe4rett\xf6myys'
        for (n,e) in events:
            if msg in e[2]:
                return True
        return False

    def runTest(self):
        self.job.wait()
        self.assertTrue(self.has_valid_event(self.job.events()))

class NonUTF8MessageTestCase(DiscoJobTestFixture, DiscoTestCase):
    inputs = [1]

    def getdata(self, path):
        return 'data\n' * 10

    @staticmethod
    def map(e, params):
        import sys
        print u'\xc4\xe4rett\xf6myys'.encode('latin-1')
        return []

    @property
    def answers(self):
        return []

    def has_valid_event(self, events):
        msg = hexlify(u'\xc4\xe4rett\xf6myys'.encode('latin-1'))
        for (n,e) in events:
            if msg in e[2]:
                return True
        return False

    def runTest(self):
        self.job.wait()
        self.assertTrue(self.has_valid_event(self.job.events()))
