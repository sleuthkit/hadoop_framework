keyType = 'long'
valueType = 'json'

def toBytes(nrd, byteOffset, blockSize):
  ret = {}
  for k in nrd:
    if (k == "flags"):
      ret[k] = nrd[k]
    else:
      ret[k] = (nrd[k] * blockSize) + byteOffset
  return ret

def mapper(key, entry, context):
  runs = None
  offset = 0
  size = 0
  good = False
  fsID = ''
  fsByteOffset = 0
  blockSize = 0
  try:
    attrs = entry['attrs']
    fsID = entry['fs_id']
    fsByteOffset = entry['fs_byte_offset']
    blockSize = entry['fs_block_size']
    for a in attrs:
      f = a['flags'] # 0x01 means in-use, 0x02 means non-resident
      t = a['type']  # 0x01 is 'default', 0x80 is 'data' on NTFS; this won't handle ADSs correctly
      if f & 0x03 > 0 and t & 0x81 > 0:
        runs = [toBytes(nrd, fsByteOffset, blockSize) for nrd in a['nrd_runs']]
        offset = runs[0]['addr']
        size = entry['size']
        good = len(runs) > 0
        break
  except Exception, ex:
    context.warning("nrd problem on %s: %s" % (key, str(ex)))
  else:
    if (good):
      context.emit(offset, {'id':key, 'fp':entry.fullPath(), 'extents':runs, 'size':size})
