keyType = 'text'
valueType = 'json'

def reducer(key, values, context):
  total = 0
  num = 0
  for val in values:
    pair = val.get()
    num += pair['count']
    total += pair['size']
  if (num > 0):
    context.emit(key, {'size':total, 'count':num})
