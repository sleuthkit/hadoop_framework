keyType = 'text'
valueType = 'long'

def mapper(key, entry, context):
  try:
    ext = entry.extension()
  except:
    pass
  else:
    context.emit(ext, 1)
