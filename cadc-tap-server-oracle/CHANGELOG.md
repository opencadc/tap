# cadc-tap-server-oracle

## 2022.05.20 - 1.2.9

  * Use proper Oracle functions for indexed columns with respect to `CONTAINS` or `INSIDE` operations. 

## 2019.10.18 - 1.2.4

  * Removed too-specific code for column replacement.
  * Added `settings.gradle` for ease of loading the repository.

## 2019.04.16 - 1.2.2

  * Added retangle (box) support for ranges.

## 2019.04.16 - 1.2.1

  * Remove full table name aliases for outer query when converting `*` with `TOP`/`MAXREC`.  Fixes [ALMA TAP #13](https://github.com/opencadc/alma-tap/issues/13). 

## 2019.04.11 - 1.2.0


  * Added requirement for Oracle Spatial Indexes.
  * Added functions to make use of Spatial Indexes.

