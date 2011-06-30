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

import md5

keyType = 'text'
valueType = 'text'

def mapper(key, entry, context):
  path = None
  hashValue = None
  good = False
  try:
    path = entry.fullPath()
    size = entry['size']
  except:
    pass
  else:
    if (size > 0):
      try:
        body = entry.getStream()
      except Exception, ex:
        context.warning("could not open %s. %s" % (path, str(ex)))
      else:
        try:
          digest = md5.new()
          s = body.read(1024)
          while (len(s) > 0):
            digest.update(s)
            s = body.read(1024)
          hashValue = digest.hexdigest()
          good = True
        except Exception, ex:
          context.warning("problem reading %s. %s" % (path, str(ex)))          
  
    if (path and hashValue and good):
      context.emit(path, hashValue)
