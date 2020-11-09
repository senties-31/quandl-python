from quandl.operations.get import GetOperation
from quandl.operations.list import ListOperation
from .data import Data
from .model_base import ModelBase
from inflection import (underscore, pluralize)

import logging
log = logging.getLogger(__name__)


class Pit(GetOperation, ListOperation, ModelBase):
    def data(self, **options):
        if not options:
            options = {'params': {}}
        return Data.page(self, **options)

    def default_path(self):
        return "%s/:id/%s" % (self.lookup_key(), self.pit_url(),)

    def lookup_key(self):
        return underscore(pluralize(type(self).__name__))

    def pit_url(self):
        interval = self.options['pit']['interval']
        if interval in ['asofdate', 'before']:
            return "%s/%s" % (interval, self.options['pit']['date'], )
        else:
            start_date = self.options['pit']['start_date']
            end_date = self.options['pit']['end_date']
            if interval == 'between':
                return "%s/%s/%s" % (interval, start_date, end_date, )
            else:
                return "%s/%s/to/%s" % (interval, start_date, end_date, )
