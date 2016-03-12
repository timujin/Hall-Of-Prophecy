import tornado
from tornado.util import basestring_type, exec_in
from tornado.escape import _unicode, native_str
from tornado.options import options, define
import random
import string

def parse_config_file(path, final=True):

    """Parses and loads the Python config file at the given path.

    This version allow customize new options which are not defined before
    from a configuration file.
    """
    config = {}
    with open(path, 'rb') as f:
        exec_in(native_str(f.read()), {}, config)
    for name in config:
        normalized = options._normalize_name(name)
        if normalized in options._options:
            options._options[normalized].set(config[name])
        else:
            tornado.options.define(name, config[name])
    if final:
        options.run_parse_callbacks()

def generateURL(size = 16, chars = string.ascii_lowercase + string.digits):
    return ''.join(random.SystemRandom().choice(chars) for _ in range(size))


