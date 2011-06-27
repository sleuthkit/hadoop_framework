#!/bin/bash

mkdir supermin.d
febootstrap --names bash coreutils udev socat -o supermin.d
echo init | cpio -o -H newc --quiet > supermin.d/init.img
febootstrap-supermin-helper -f ext2 supermin.d x86_64 kernel initrd root

