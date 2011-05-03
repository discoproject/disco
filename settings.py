import os

class Settings(dict):
    """
    A dictionary for storing settings.

    Provides special mechanisms for setting and overriding default values.
    Defaults can be overridden by a settings file and/or the environment.
    """
    defaults = {}
    globals  = globals()
    settings_file_var = None

    def __init__(self, *args, **kwargs):
        super(Settings, self).__init__(*args, **kwargs)
        self.settings_file_defs = {}
        if self.settings_file_var:
            settings_file = self[self.settings_file_var]
            if os.path.exists(settings_file):
                execfile(settings_file, {}, self.settings_file_defs)

    def __getitem__(self, key):
        """Get `key`: check the instance, then the env, then defaults."""
        if key in self:
            return super(Settings, self).__getitem__(key)
        if key in os.environ:
            return os.environ[key]
        if key in self.settings_file_defs:
            return self.settings_file_defs[key]
        return eval(self.defaults[key], self.globals, self)

    def __reduce__(self):
        return type(self), (dict((k, self[k]) for k in self.defaults),)

    def safedir(self, key):
        """Make sure the directory path stored in the setting `key` exists."""
        path = self[key]
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    @property
    def env(self):
        env = os.environ.copy()
        env.update((k, str(self[k])) for k in self.defaults)
        return env
