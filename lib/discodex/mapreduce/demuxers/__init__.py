from .. import module

@module(__package__)
def namedfielddemux(record, params):
    return zip(record.fieldnames, record.fields)
