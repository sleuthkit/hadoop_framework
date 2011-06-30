# /*
#    Copyright 2011, Lightbox Technologies, Inc
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
# */

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
