RELEASE_VERSION:=$(shell bash get_sdk_version.sh)

.PHONY: all
all:
	@echo "Nothing yet (RELEASE_VERSION=$(RELEASE_VERSION))"

.PHONY: clean
clean:
	rm -f $(shell find . -type f -name '*.unc-backup~')
	rm -rf apps/TestNetworkProxy/build apps/TestNetworkProxy/app/build

.PHONY: uncrustify-replace
uncrustify-replace:
	uncrustify -c style.cfg --replace $(shell find src/ -type f -name '*.java')
	uncrustify -c style.cfg --replace $(shell find apps/ -type f -name '*.java')

.PHONY: uncrustify-check
uncrustify-check:
	uncrustify -c style.cfg --check $(shell find src/ -type f -name '*.java')
	uncrustify -c style.cfg --check $(shell find apps/ -type f -name '*.java')

# For travis's old uncrustify which lacks --check.
.PHONY: uncrustify-check-legacy
uncrustify-check-legacy:
	uncrustify -c style.cfg --replace --no-backup $(shell find src/ -type f -name '*.java')
	uncrustify -c style.cfg --replace --no-backup $(shell find apps/ -type f -name '*.java')
	git diff --exit-code || false

.PHONY: fixme-check
fixme-check:
	if grep FIXME $(shell find src/ -type f -name '*.java'); then echo Found FIXMEs; false; fi
	if grep FIXME $(shell find apps/ -type f -name '*.java'); then echo Found FIXMEs; false; fi

.PHONY: dist-archive
dist-archive:
	git archive --format zip -o /tmp/npay-np-android-sdk-$(RELEASE_VERSION).zip --prefix npay-np-android-sdk-$(RELEASE_VERSION)/ HEAD
