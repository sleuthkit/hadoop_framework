keyType = 'text'
valueType = 'text'

def mapper(key, entry, context):
  try:
    fp = entry.fullPath()
    ext = entry.extension()
  except:
    pass
  else:
    context.emit(ext, fp)
