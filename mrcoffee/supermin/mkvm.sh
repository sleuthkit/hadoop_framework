#!/bin/bash

set -x
mkdir supermin.d
febootstrap --names bash coreutils udev socat -o supermin.d
echo init | cpio -o -H newc --quiet > supermin.d/init.img
echo mrcoffee | cpio -o -H newc --quiet >supermin.d/mrcoffee.img
febootstrap-supermin-helper -f ext2 supermin.d x86_64 kernel initrd root

