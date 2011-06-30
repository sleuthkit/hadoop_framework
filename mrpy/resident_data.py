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
