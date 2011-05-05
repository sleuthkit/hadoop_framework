keyType = 'text'
valueType = 'json'

def mapper(key, entry, context):
  try:
    ext = entry.extension()
    size = entry['size']
    context.emit(ext, {'size':size, 'count':1})
  except:
    pass
