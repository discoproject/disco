import os

class Settings(dict):
    """
    A dictionary for storing settings.

    Provides special mechanisms for setting and overriding default values.
    Defaults can be overridden by a settings file and/or the environment.
    """
    settings_file_var = None
    defaults = {}

    def __init__(self, **kwargs):
        super(Settings, self).__init__(kwargs)
        if self.settings_file_var:
            execfile(self[self.settings_file_var], {}, self)

    def __getitem__(self, key):
        """Get the setting `key`, first checking the environment, then the instance."""
        if key in os.environ:
            return os.environ[key]
        if key not in self:
            return eval(self.defaults[key], globals(), self)
        return super(Settings, self).__getitem__(key)

    def safedir(self, key):
        """Make sure the directory path stored in the setting `key` exists."""
        path = self[key]
        if not os.path.exists(path):
            os.makedirs(path)
        return path

    @property
    def env(self):
        settings = os.environ.copy()
        settings.update(dict((k, str(self[k])) for k in self.defaults))
        return settings
