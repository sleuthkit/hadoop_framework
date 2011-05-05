import random

keyType = 'text'
valueType = 'text'

def reducer(key, values, context):
  """A Hadoop reducer to choose a single value from several associated with a given key.
  
  Arguments
  key -- The key from Hadoop.
  values -- An iterable sequence of values associated with the key.
  context -- An object with a function, emit(), that can be passed output key, value pairs
  
  Key will be unboxed from its Writable container, e.g. Text is passed as a string, and LongWritable as an [Python] int.
  Values should be iterated in a for-loop. Items in values are _not_ unboxed, so they will inherit from Writable.
  """
  numValues = 0
  choice = None
  for val in values:
    numValues += 1
    if (random.randint(1, numValues) == 1):
      choice = val
  context.emit(key, choice)
