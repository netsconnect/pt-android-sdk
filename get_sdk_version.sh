#!/bin/sh
SDK_VERSION=`grep 'SDK_VERSION =' src/com/napy/np/android/sdk/Sdk.java | gawk 'match($0, /SDK_VERSION = "(.*?)"/, a) { print a[1] }'`
echo $SDK_VERSION
