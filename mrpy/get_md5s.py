keyType = 'text'
valueType = 'text'

def mapper(key, entry, context):
  try:
    path = entry.fullPath()
    md5 = entry['md5']
  except:
    pass
  else:
    if (path and md5):
      context.emit(path, md5)
