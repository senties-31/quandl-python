from quandl.model.pit import Pit
from quandl.errors.quandl_error import LimitExceededError
from .api_config import ApiConfig
from .message import Message
import warnings
import copy


def get_pit_data(datatable_code, **options):
    validate_pit_options(options)
    pit_options = {}

    # Remove the PIT params/keys from t he options to not send it as a query params
    for k in ['interval', 'date', 'start_date', 'end_date']:
        if k in options.keys():
            pit_options[k] = options.pop(k)

    if 'paginate' in options.keys():
        paginate = options.pop('paginate')
    else:
        paginate = None

    data = None
    page_count = 0
    while True:
        next_options = copy.deepcopy(options)
        next_data = Pit(datatable_code, pit=pit_options).data(params=next_options)

        if data is None:
            data = next_data
        else:
            data.extend(next_data)

        if page_count >= ApiConfig.page_limit:
            raise LimitExceededError(
                Message.WARN_DATA_LIMIT_EXCEEDED % (datatable_code,
                                                    ApiConfig.api_key
                                                    )
            )

        next_cursor_id = next_data.meta['next_cursor_id']

        if next_cursor_id is None:
            break
        elif paginate is not True and next_cursor_id is not None:
            warnings.warn(Message.WARN_PAGE_LIMIT_EXCEEDED, UserWarning)
            break

        page_count = page_count + 1
        options['qopts.cursor_id'] = next_cursor_id
    return data.to_pandas()


def validate_pit_options(options):
    if 'interval' not in options.keys():
        # TODO: Change it to raise the correct error
        raise AttributeError('option `interval` is required')

    if options['interval'] in ['asofdate', 'before']:
        # check for date
        if 'date' not in options.keys():
            # TODO: Change it to raise the correct error
            raise AttributeError('option `date` is required')
    elif options['interval'] in ['from', 'between']:
        # check for dates
        if 'start_date' not in options.keys() or 'end_date' not in options.keys():
            # TODO: Change it to raise the correct error
            raise AttributeError('options `start_date` and `end_date` are required')
    else:
        raise AttributeError('option `interval` is invalid')
