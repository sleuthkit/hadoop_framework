keyType = 'text'
valueType = 'long'

def mapper(key, entry, context):
  try:
    path = entry['path']
  except:
    pass
  else:
    context.emit(path, 1)
