#!/bin/bash

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

set -x
mkdir supermin.d
febootstrap --names bash coreutils udev socat -o supermin.d
echo init | cpio -o -H newc --quiet > supermin.d/init.img
echo mrcoffee | cpio -o -H newc --quiet >supermin.d/mrcoffee.img
febootstrap-supermin-helper -f ext2 supermin.d x86_64 kernel initrd root

