from .. import module

@module(__name__)
def namedfielddemux(record, params):
    return zip(record.fieldnames, record.fields)
