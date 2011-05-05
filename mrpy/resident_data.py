keyType = 'text'
valueType = 'json'

def mapper(key, entry, context):
  good = False
  data = None
  try:
    attrs = entry['attrs']
    for a in attrs:
      if a['flags'] & 4 == 4 and a['type'] & 0x80 and a['name'] == "$Data":
        data = a
        good = True
        break
  except:
    pass
  else:
    if (good):
      context.emit(key, data)
