# =================================================================
#
# Author: Gabriel de Courval-Paré
#
# Copyright (c) 2023 Tom Kralidis
# Copyright (c) 2025 Gabriel de Courval-Paré
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

import logging
import pandas as pd
from shapely import from_wkb

LOGGER = logging.getLogger(__name__)

PROCESS_METADATA = {
    'version': '0.1.0',
    'id': 'wind-data',
    'title': 'GeoMet-Weather get spectra geoparquet process',
    'description': 'GeoMet-Weather get spectra geoparquet process',
    'keywords': ['spectra geoparqet'],
    'links': [],
    'inputs': {
        'model': {
            'title': 'model name',
            'description': 'GDWPS, GEWPS, RDWPS, REWPS',
            'schema': {
                'type': 'string',
            },
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'variable': {
            'title': 'variable',
            'description': 'EFTH for 2d model, F, STH1M, TH1M for 1d model',
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'date': {
            'title': 'date',
            'description': 'Date in yearmonthdayhour format',
            'schema': {'type': 'string'},
            'minOccurs': 1,
            'maxOccurs': 1
        },
        'columns': {
            'title': 'columns',
            'description': 'The names of the columns you want from the geoparquet, separated by ;',
            'schema': {
                'type': 'string'
            },
            'nullable': True,
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'filters': {
            'title': 'filters',
            'description': 'The filters you want to apply to the geoparquet as specified in https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html',
            'schema': {
                'type': 'array',
                'items': {
                    'type': 'array',
                    'items': {
                        'oneOf': {
                            'type': 'string',
                            'type': 'number'
                        }
                    }
                }
            },
            'nullable': True,
            'minOccurs': 0,
            'maxOccurs': 1
        },
        'remove_duplicates': {
            'title': 'remove duplicates',
            'description': 'Set to true to remove duplicates from geoparquet',
            'schema': {
                'type': 'boolean'
            },
            'nullable': True,
            'minOccurs': 0,
            'maxOccurs': 1
        }
    },
    'outputs': {
        'get_spectra_geoparquet_response': {
            'title': 'get_spectra_geoparquet_response',
            'schema': {'contentMediaType': 'application/json'}
        }
    },
    'example':{
        'inputs':{
            'model': 'GEWPS',
            'variable': 'EFTH',
            'date': '2025031500',
            'columns': 'station_name;member',
            'filters': [["station_name", "==", "00N000E"], ["member", "==", 0]],
        }
    }
}

MAIN_URL = "https://goc-dx.science.gc.ca/~swav000/geoparquet"

def get_spectra_geoparquet(
        model,
        variable,
        date,
        region=None,
        columns=None,
        filters=None,
        remove_duplicates=None
):
    output = {}

    if columns is not None:
        columns = columns.split(';')

    if filters is not None:
        filters = [tuple(filter) for filter in filters]

    if model.lower() in ("gewps", "gdwps"):
        path = f"{MAIN_URL}/{model.lower()}/{date}_MSC_{model.upper()}_{variable.upper()}.geoparquet"
    elif model.lower() in ("rdwps", "rewps"):
        path = f"{MAIN_URL}/{model.lower()}/{date}_MSC_{model.upper()}-{region}_{variable.upper()}.geoparquet"
    else:
        raise ValueError(f"Invalid model {model}")

    try:
        df = pd.read_parquet(path, engine='pyarrow', columns=columns, filters=filters)
    except FileNotFoundError as e:
        raise FileNotFoundError(f"File not found: {e}")
    except pd.errors.EmptyDataError as e:
        raise pd.errors.EmptyDataError(f"Empty data: {e}")
    except Exception as e:
        raise Exception(f"An unexpected error occurred: {e}")

    if remove_duplicates:
        df = df.drop_duplicates(ignore_index=True)

    if "geometry" in df.columns:
        geom = df[:]["geometry"]
        df = df.drop(columns=["geometry"])
        geom = geom.map(lambda g: from_wkb(g))
        geom = geom.map(lambda p: {'y': p.y, 'x': p.x})

        df["point"] = geom
        df.reset_index(inplace=True)
        df = df.drop(columns=["index"])
    
    df = df.to_dict()
    output = df
    
    return output

try:
    from pygeoapi.process.base import BaseProcessor, ProcessorExecuteError

    class GetSpectraGeoparquetProcessor(BaseProcessor):
        """Get spectra geoparquet processor"""

        def __init__(self, provider_def):
            """
            Initialize object

            :param provider_def: provider definition

            :returns: pygeoapi.process.weather.get_spectra_geoparquet.GetSpectraGeoparquetProcessor  # noqa
            """

            BaseProcessor.__init__(self, provider_def, PROCESS_METADATA)

        def execute(self, data, outputs=None):
            model = data.get("model")
            if model.lower() in ("gdwps", "gewps"):
                required = ("model", "variable", "date")
            elif model.lower() in ("rdwps", "rewps"):
                required = ("model", "variable", "date", "region")
            else:
                raise ValueError(f"Invalid model {model}")

            if not all([param in data for param in required]):
                msg = "Missing required parameters."
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)
            
            var = data.get("variable")
            date = data.get("date")
            region = data.get("region")
            cols = data.get("columns")
            filters = data.get("filters")
            remove_duplicates = data.get("remove_duplicates")

            mimetype = "application/json"

            try:
                output = get_spectra_geoparquet(
                    model,
                    var,
                    date,
                    region,
                    cols,
                    filters,
                    remove_duplicates
                )
            except ValueError as err:
                msg = f'Process execution error: {err}'
                LOGGER.error(msg)
                raise ProcessorExecuteError(msg)

            return mimetype, output
        
        def __repr__(self):
            return f'<ExtractWindDataProcessor> {self.name}'
        
except (ImportError, RuntimeError) as err:
    LOGGER.warning(f'Import errors: {err}')