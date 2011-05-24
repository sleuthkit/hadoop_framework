keyType = 'text'
valueType = 'bytes'

def mapper(key, entry, context):
  try:
    path = entry.fullPath()
    sha1 = entry['sha1']
  except:
    pass
  else:
    if (path and sha1):
      context.emit(path, sha1)
