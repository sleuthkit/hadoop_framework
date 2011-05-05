keyType = 'text'
valueType = 'long'

def mapper(key, entry, context):
  try:
    path = entry['path']
    size = entry['size']
  except:
    pass
  else:
    context.emit(path, size)
