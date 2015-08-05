PROJDIR = $(realpath $(CURDIR)/..)
include $(PROJDIR)/build/common.mk

TOOLS_DIR = $(PROJDIR)/tools/devrel
RIAK_CONF = $(RIAK_DIR)/etc/riak.conf
ADV_CONF = $(RIAK_DIR)/etc/advanced.config
RIAK_ADMIN = $(RIAK_DIR)/bin/riak-admin

export RIAK_HOST = localhost
export RIAK_PORT = 8087

preconfigure:
	$(TOOLS_DIR)/gen-riak-conf $(RIAK_CONF) 8098 8087
	$(TOOLS_DIR)/strong-consistency-conf $(RIAK_CONF)
	$(TOOLS_DIR)/gen-adv-conf $(ADV_CONF)

configure:
	$(TOOLS_DIR)/riak-cluster-config $(RIAK_ADMIN) 8098 true false

compile: install-deps

setup-integration:
	echo 'Nothing to do!'

