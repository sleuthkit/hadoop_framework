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

keyType = 'text'
valueType = 'json'

def mapper(key, entry, context):
  good = False
  out = None
  try:
    if (entry.extension() == 'txt' and entry['path'].find("Cookies") > 0):
      data = entry.getStream() # can also pass in "Content" as specifier
      lines = data.readlines()
      if (len(lines) >= 4):
        keys = ['name', 'value', 'domain', 'flags']
        vals = [l.strip() for l in lines[:4]] # gets the first four items in list, and removes leading/trailing whitespace
        out = dict(zip(keys, vals)) # zip takes two lists and returns a list of pairs, which dict() turns into a dictionary
        good = True
  except:
    pass
  else:
    if (good):
      context.emit(key, out)
