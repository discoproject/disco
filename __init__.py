"""
from PROJECT.settings import ProjectSettings
>>> class ConcreteProgram(Program):
...     pass

>>> @ConcreteProgram.command
... def test(*args, **options):
...     print 'command', args

>>> @test.subcommand('subcommand')
... def sub_command(*args, **options):
...     print 'named subcommand'

>>> ConcreteProgram(['test']).main()
command ()
>>> ConcreteProgram(['test', 'subcommand']).main()
named subcommand
>>> ConcreteProgram(['test', 'sub_command']).main()
command ('sub_command',)
>>> list(x[0] for x in walk(ConcreteProgram.commands))
['test', 'test subcommand']
"""
import optparse, os, sys
from functools import partial

from cli.settings import Settings

class OptionParser(optparse.OptionParser):
    def __init__(self, **kwargs):
        optparse.OptionParser.__init__(self, add_help_option=False, **kwargs)
        self.add_option('-h', '--help',
                        action='store_true',
                        help='display command help')
        self.add_option('-s', '--settings',
                        help='use settings file settings')
        self.add_option('-v', '--verbose',
                        action='store_true',
                        help='print debugging messages')

def command(obj, name_or_function):
    def _command(obj, name, function):
        obj.commands[name] = Command(function)
        return obj.commands[name]
    if isinstance(name_or_function, basestring):
        return partial(_command, obj, name_or_function)
    return _command(obj, name_or_function.__name__, name_or_function)

def walk(commands):
    for name, command in commands.iteritems():
        yield name, command
        for subname, subcommand in command.commands.iteritems():
            yield ('%s %s' % (name, subname)), subcommand

def search(receiver, commands):
    for n, command in enumerate(commands):
        if command not in receiver.commands:
            return receiver, commands[n:]
        receiver = receiver.commands[command]
    return receiver, ()

class Command(object):
    subcommand = command

    def __init__(self, function):
        self.commands = {}
        self.function = function

    def __call__(self, *args, **opts):
        return self.function(*args, **opts)

    def __str__(self):
        return self.function.__doc__ or 'Not documented'

class Program(object):
    commands       = {}
    command        = classmethod(command)
    settings_class = Settings

    def __init__(self, argv=sys.argv[1:], option_parser=OptionParser()):
        option_parser.usage     = self.usage
        self.option_parser      = option_parser
        self.options, self.argv = option_parser.parse_args(argv)

    def __call__(self, *args, **opts):
        return self.default(*args, **opts)

    def __str__(self):
        return self.default.__doc__ or self.option_parser.format_help()

    @property
    def name(self):
        return self.__class__.__name__.lower()

    @property
    def usage(self):
        names = [' %s' % name for name, command in walk(self.commands)]
        return ''.join('\n\t%s%s [options]' % (self.name, name)
                       for name in [''] + names)

    def default(self, *args, **opts):
        print("No default command set. Override the default command in your program subclass.")

    def dispatch(self, options, argv):
        if options.settings:
            if not self.settings_class.settings_file_var:
                raise Exception("%s does not use a settings file" % self.settings_class)
            os.setenv(self.settings_class.settings_file_var, options.settings)
        options.settings = self.settings = self.settings_class()

        if options.verbose:
            print(
                """
                %s settings are:

                %s

                If this is not what you want, see the `--help` option
                """ % (self.name,
                       '\n\t\t'.join('%s = %s' % item
                                     for item in sorted(self.settings.env.iteritems()))))

        command, args = search(self, argv)

        if options.help:
            print("%s" % command)
        else:
            command(*args, **options.__dict__)

    def main(self):
        try:
            return self.dispatch(self.options, self.argv)
        except Exception, e:
            print e
