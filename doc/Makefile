# Minimal makefile for Sphinx documentation
# This file was generated with sphinx-quickstart version 3.5.0
#

# You can set these variables from the command line, and also
# from the environment for the first two.
SPHINXOPTS    ?=
SPHINXBUILD   ?= sphinx-build
SOURCEDIR     = source
BUILDDIR      = build

# Put it first so that "make" without argument is like "make help".
help:
	@$(SPHINXBUILD) -M help "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)

clean: 
	exec rm -rf "$(BUILDDIR)"

multiversion:
	sphinx-multiversion "$(SOURCEDIR)" "$(BUILDDIR)"
	
.PHONY: help clean Makefile

# Catch-all target: route all unknown targets to Sphinx using the new
# "make mode" option.  $(O) is meant as a shortcut for $(SPHINXOPTS).
%: Makefile 
	ixmp4 server dump-schema -o "$(SOURCEDIR)/openapi-v1.json"
	@$(SPHINXBUILD) -M $@ "$(SOURCEDIR)" "$(BUILDDIR)" $(SPHINXOPTS) $(O)
