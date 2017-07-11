import logging
import logging.config

class SpiderFilter(logging.Filter):

    def __init__(self, allow=None, disable=None):
        self.allow_channels = allow
        self.disable_channels = disable

    def filter(self, record):
        if self.allow_channels is not None:
            if record.name in self.allow_channels:
                allow = True
            else:
                allow = False
        elif self.disable_channels is not None:
            if record.name in self.disable_channels:
                allow = False
            else:
                allow = True
        else:
            allow = False
        return allow


LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        'verbose': {
            'format': '%(levelname)s %(asctime)s %(module)s %(process)d %(thread)d %(message)s'
        },
        'simple': {
            'format': '%(asctime)s -- %(name)s !!!%(levelname)s!!!: %(message)s'
        },
    },
    'filters': {
        'basic': {
            '()': SpiderFilter,
            'allow': ('mongo', 'redis', 'mysql'),
        },
        'warn': {
            '()': SpiderFilter,
            'disable': ()
        }
    },
    'handlers': {
        'file': {
            'level': 'WARN',
            'formatter': 'simple',
            'class': 'logging.FileHandler',
            'filename': 'spider.log',
            'mode': 'a',
            'filters': ['warn']
        },
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'simple'
        },
        'database': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'formatter': 'simple',
            'filename': 'spider.log',
            'mode': 'a',
            'filters': ['basic']
        }
    },
    'loggers': {
        'mongo': {
            'handlers':['file'],
            'propagate': True,
            'level':'ERROR',
        },
        'mysql': {
            'handlers': ['database'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'redis': {
            'handlers': ['console', 'database'],
            'level': 'INFO',
            'filters': ['basic']
        }
    },
    'root': {
        'level': 'DEBUG',
        'handlers': ['console']
    }
}

if __name__ == '__main__':
    logging.config.dictConfig(LOGGING)
    logging.getLogger('mysql').debug('Simple Log Test!')