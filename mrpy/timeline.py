keyType = 'long'
valueType = 'text'

timestamps = 'created accessed written updated'.split()

def mapper(key, entry, context):
  for ts in timestamps:
    try:
      t = entry[ts]
      unixT = t.getTime() / 1000
      context.emit(unixT, '%s: %s' % (ts, entry.fullPath()))
    except:
      pass
