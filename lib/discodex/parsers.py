def map(entry, params):
    def parse(record, params):
        yield 'key', 'value'
    return [item for e in entry for item in parse(e, params)]
