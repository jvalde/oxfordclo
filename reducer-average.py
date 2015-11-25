#! /usr/bin/env python
import sys

speeds = dict()
stations = dict()
average = dict()

for line in sys.stdin:
    try:

      line = line.strip()
      station, speed = line.split('\t')

      speed = float(speed)
      stationCount = stations.get(station, 0) + 1
      stations[station] = stationCount
      sumSpeed = speeds.get(station, 0) + speed
      speeds[station] = sumSpeed
      averageSpeed = sumSpeed / stationCount
      average[station] = averageSpeed

    except ValueError:
      pass

for k, v in average.iteritems():
      result = [ k, str(v) ]
      print ('\t'.join(result))