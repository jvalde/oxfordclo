#! /usr/bin/env python
import sys

speeds = dict()
stations = dict()

for line in sys.stdin:
    try:

      line = line.strip()
      station, speed = line.split('\t')

      speed = float(speed)
      storedSpeed = stations.get(station, 0)
      if speed > storedSpeed :
        stations[station] = speed

    except ValueError:
      pass

for k, v in stations.iteritems():
      result = [ k, str(v) ]
      print ('\t'.join(result))