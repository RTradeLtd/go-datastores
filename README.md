# go-datastores

[![codecov](https://codecov.io/gh/RTradeLtd/go-datastores/branch/master/graph/badge.svg)](https://codecov.io/gh/RTradeLtd/go-datastores) [![Build Status](https://travis-ci.com/RTradeLtd/go-datastores.svg?branch=master)](https://travis-ci.com/RTradeLtd/go-datastores) [![RTradeLtd](https://circleci.com/gh/RTradeLtd/go-datastores.svg?style=shield)](https://app.circleci.com/pipelines/github/RTradeLtd/go-datastores) [![Go Report Card](https://goreportcard.com/badge/github.com/RTradeLtd/go-datastores)](https://goreportcard.com/report/github.com/RTradeLtd/go-datastores)


`go-datastores` is a collection of a variety of IPFS datastores to be used by TemporalX in a single monorepo for easy maintenance. A majority of these datastores are forked from upstream repositories, with minor modifications to faciltiate easier integration with TemporalX, along with performance improvements and optimizations where possible. Additionally it allows us to pull in all datastores we need from a single repository.

If you are a user of TemporalX and want to be able to use datastores that we do not yet support, you can submit a PR and we'll enable usage of the datastore within our next release

# supported datastores

## full support

The following datastores are marked as "fully supported" and should have no problems with usage outside of edge case bugs

* badger
* leveldb
* sql
  * includes a postgresql implementation
  * note this may have unknown issues

##  partial support

The following datastores are marked as "partially supported" and may have problems with usage, namely related to query functionality.

* pebble
  * functions supported:
    * Get
    * GetSize
    * Delete
    * Put
    * Has
  * problematic:
    * Query
* nutsdb
  * functions supported:
    * Get
    * GetSize
    * Delete
    * Put
    * Has
  * problematic:
    * Query

# license

Some of the datastores were forked from upstream, and as such are licensed under the upstream licenses, any forked datastores will have a `LICENSE.orig` in their folder to indicate the corresponding upstream license. Any datastores without the `LICENSE.orig` in their folders are licensed under AGPL-v3 license, for which you can find in the `LICENSE` file in the repository root.