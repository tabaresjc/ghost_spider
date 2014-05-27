# -*- coding: utf-8 -*-
import os.path
import csv, codecs, cStringIO


class CsvWriter:
  """
  A CSV writer which will write rows to CSV file "f",
  which is encoded in the given encoding.
  """

  def __init__(self, f, dialect=csv.excel, encoding="utf-8", **kwds):
    self.queue = cStringIO.StringIO()
    self.writer = csv.writer(self.queue, dialect=dialect, **kwds)
    self.stream = f
    self.encoder = codecs.getincrementalencoder(encoding)()

  @classmethod
  def write_to_csv(cls, filename, rows, firs_row=[]):
    is_blank = not os.path.isfile(filename)
    csvw = cls(open(filename, 'a+'), dialect='excel')
    if is_blank and firs_row:
      csvw.writerow(firs_row)
    csvw.writerow(rows)

  def writerow(self, row):
    self.writer.writerow([s.encode("utf-8") for s in row])
    # Fetch UTF-8 output from the queue ...
    data = self.queue.getvalue()
    data = data.decode("utf-8")
    # ... and reencode it into the target encoding
    data = self.encoder.encode(data)
    # write to the target stream
    self.stream.write(data)
    # empty queue
    self.queue.truncate(0)

  def writerows(self, rows):
    for row in rows:
      self.writerow(row)
