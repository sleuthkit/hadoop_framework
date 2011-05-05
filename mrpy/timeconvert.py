import time

inKeyType = 'long'
keyType = 'text'
valueType = 'text'

def reducer(key, values, context):
  timestamp = time.ctime(key)
  for val in values:
    context.emit(timestamp, val.get())
