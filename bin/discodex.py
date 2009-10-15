#!/usr/bin/env python
"""
Module/script for controlling the discodex.
"""

import optparse, os, sys

commands = {'client': set(('list',
                           'get',
                           'put',
                           'delete',
                           'index',
                           'clone',
                           'keys',
                           'values',
                           'query')),
            'server': set(('status',
                           'start',
                           'stop',
                           'restart')),
            }

def main():
    DISCODEX_BIN  = os.path.dirname(os.path.realpath(__file__))
    DISCODEX_HOME = os.path.dirname(DISCODEX_BIN)

    option_parser = optparse.OptionParser()
    option_parser.add_option('-s', '--settings',
                             help='use settings file settings')
    option_parser.add_option('-v', '--verbose',
                             action='store_true',
                             help='print debugging messages')
    option_parser.add_option('-p', '--print-env',
                             action='store_true',
                             help='print the parsed environment and exit')
    option_parser.add_option('-H', '--host',
                             help='host that the client should connect to')
    option_parser.add_option('-P', '--port',
                             help='port that the client should connect to')
    option_parser.add_option('--parser',
                             default='rawparse',
                             help='parser object to use for indexing')
    option_parser.add_option('--demuxer',
                             default='nodemux',
                             help='demuxer object to user for indexing')
    option_parser.add_option('--balancer',
                             default='nchunksbalance',
                             help='balancer to use for indexing')
    option_parser.add_option('--nr-ichunks',
                             default='8',
                             help='the number of ichunks to create')
    option_parser.add_option('--no-sort',
                             action='store_true',
                             help='whether or not to sort before writing discodbs')
    option_parser.add_option('--k-viter',
                             action='store_true',
                             help='treat the dataset as a precomputed k-viter list')
    options, sys.argv = option_parser.parse_args()

    if options.settings:
        os.setenv('DISCODEX_SETTINGS', options.settings)
    sys.path.insert(0, os.path.join(DISCODEX_HOME, 'lib'))
    from discodex.settings import DiscodexSettings
    discodex_settings = DiscodexSettings()

    if options.verbose:
        print(
            """
            It seems that Discodex is at {DISCODEX_HOME}
            Discodex settings are at {0}

            If this is not what you want, see the `--help` option
            """.format(options.settings, **discodex_settings))

    if options.print_env:
        for item in sorted(discodex_settings.env.iteritems()):
            print('%s = %s' % (item))
        sys.exit(0)

    argdict = dict(enumerate(sys.argv))
    command = argdict.pop(0, 'status')

    if command in commands['client']:
        from discodex.client import CommandLineClient
        receiver = CommandLineClient(options.host or discodex_settings['DISCODEX_HTTP_HOST'],
                                     options.port or discodex_settings['DISCODEX_HTTP_PORT'],
                                     options)
    elif command in commands['server']:
        from discodex.server import djangoscgi
        receiver = djangoscgi(discodex_settings)
    else:
        raise NameError('Unrecognized command: %s' % command)

    for message in receiver.send(command, *sys.argv[1:]):
        print(message)

if __name__ == '__main__':
    try:
        main()
    except Exception, e:
        print('Discodex encountered an unexpected error:')
        raise
