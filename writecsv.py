import os
import sys
import json
import struct
import csv

from ross_vis.DataModel import RossData
from ross_vis.DataCache import RossDataCache as Cache

from tornado.options import define, options, parse_command_line

define("output", help="output file", type=str)
define("input", help="input file", type=str)
define("granu", default='PeData', help="granularity: PeData, KpData or LpData", type=str)

if __name__ == '__main__':
  parse_command_line()
  
  if (os.path.isfile(options.input)):
    
    if (options.granu in ['PeData', 'KpData', 'LpData']):
      cache = Cache()
      cache.loadfile(options.input)
      data = cache.export_dict(options.granu)

      with open(options.output, 'w') as f:
        w = csv.DictWriter(f, data[0].keys())
        w.writeheader()
        for d in data:
          w.writerow(d)


           