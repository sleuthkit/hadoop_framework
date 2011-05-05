import md5

keyType = 'text'
valueType = 'text'

def mapper(key, entry, context):
  path = None
  hashValue = None
  good = False
  try:
    path = entry.fullPath()
    size = entry['size']
  except:
    pass
  else:
    if (size > 0):
      try:
        body = entry.getStream()
      except Exception, ex:
        context.warning("could not open %s. %s" % (path, str(ex)))
      else:
        try:
          digest = md5.new()
          s = body.read(1024)
          while (len(s) > 0):
            digest.update(s)
            s = body.read(1024)
          hashValue = digest.hexdigest()
          good = True
        except Exception, ex:
          context.warning("problem reading %s. %s" % (path, str(ex)))          
  
    if (path and hashValue and good):
      context.emit(path, hashValue)
