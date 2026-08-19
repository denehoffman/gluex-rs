# Changelog

## [0.1.7](https://github.com/denehoffman/gluex-rs/compare/gluex-rs-v0.1.0...gluex-rs-v0.1.7) (2026-08-19)


### ⚠ BREAKING CHANGES

* unify workspace into one crate
* move CLI ownership to gluex facade
* consolidate Python bindings into gluex package
* audit facade APIs for unified bindings
* align REST/timestamp APIs across crates and Python bindings
* **gluex-lumi:** introduce Context/Luminosity API and update CLI

### Features

* Add argument for skipping runs in gluex-lumi ([f45cf5e](https://github.com/denehoffman/gluex-rs/commit/f45cf5ee513a729834ab18d1cdf0f1da17dbac0a))
* Add batched writing with proper event numbers ([1782f89](https://github.com/denehoffman/gluex-rs/commit/1782f895cebdc18e9abc9a7bbb80263a62af4684))
* Add convenience methods for creating and filling histograms ([22b162e](https://github.com/denehoffman/gluex-rs/commit/22b162eb7e89be5c3caee7b15eac2a1ffed23380))
* Add convenience methods for Data and improve access performance ([614e151](https://github.com/denehoffman/gluex-rs/commit/614e1515431c0e087f4ffed03f239eb58ff17975))
* Add fetch method which incorporates REST version timestamps and run period selection ([ef0a6eb](https://github.com/denehoffman/gluex-rs/commit/ef0a6eb25acb77d743f790c5923bf940198a5470))
* Add generate-import-lib feature to PyO3 to enable pypy builds on windows x64 ([0ce3c68](https://github.com/denehoffman/gluex-rs/commit/0ce3c68dcb5a29d6b0b25fc3b3ffee8bdbb2f323))
* Add gluex hddm generation example ([fbddaca](https://github.com/denehoffman/gluex-rs/commit/fbddacab3871f20adf7b8ccf791f393827769669))
* Add gluex mc generation crate ([08095f4](https://github.com/denehoffman/gluex-rs/commit/08095f455728ac84b7a0e630da582ddd5c4e9f02))
* Add prelude, use CCDBResult alias, and add column_types to RowView and column iterators ([8d2c96a](https://github.com/denehoffman/gluex-rs/commit/8d2c96af1592ce844a6b99a29cb12199eeee9814))
* Add python interface, rename Database to CCDB, and add a lot of helpers/alternate methods. rename subdir(s) to dir(s) ([09508a5](https://github.com/denehoffman/gluex-rs/commit/09508a5c356b224371bf429583f87d496b838b1a))
* Add run period arguments to fetch and fix aliases type hinting ([91e128b](https://github.com/denehoffman/gluex-rs/commit/91e128b059c385f3d6f2e7951ae1107144b141e1))
* Add run_range and contains methods to RunPeriod ([b949014](https://github.com/denehoffman/gluex-rs/commit/b9490143127543ade2a9d240a0ff614c05656863))
* Align REST/timestamp APIs across crates and Python bindings ([ce01c18](https://github.com/denehoffman/gluex-rs/commit/ce01c1835c8761c374c29db580e9805dd97c7f56))
* Cache queries and variations, and ensure the temp database for run number requests is in-memory ([0e10661](https://github.com/denehoffman/gluex-rs/commit/0e1066117ffeb28923bb83dd27f2799430085906))
* Consolidate Python bindings into gluex package ([414dfc3](https://github.com/denehoffman/gluex-rs/commit/414dfc3fb1ef91e02478da488d8991ffc2c744ab))
* **core:** Add equivalent of particleType.h ([f67ab2e](https://github.com/denehoffman/gluex-rs/commit/f67ab2eaf5dd0b7285aef95c28fdb9f2b6b02474))
* **core:** Add shared path resolver and adopt across crates ([c2c9620](https://github.com/denehoffman/gluex-rs/commit/c2c9620ca0b396acb048fec5c3d595d6e8ef3ac2))
* **detectors.rs:** Add enums for dealing with GlueX detectors ([1e8f2b0](https://github.com/denehoffman/gluex-rs/commit/1e8f2b0cb59d03ab8bb243b3566d54893711ea5c))
* Establish unified Python luminosity package ([ad9dfe5](https://github.com/denehoffman/gluex-rs/commit/ad9dfe538117491574e0d9b63f0d930a9469162e))
* Expose shared core types in unified Python package ([edd9f57](https://github.com/denehoffman/gluex-rs/commit/edd9f57fb0ff2c2f20c3008790e9d265b5c9b8a1))
* First draft of RCDB function, move some constants into gluex-core ([dfda19d](https://github.com/denehoffman/gluex-rs/commit/dfda19d5c7a747562d931f73663d858799bf7c87))
* First full impl of gluex-lumi, but it's slow due to RCDB, and gluex-ccdb-py won't build ([7b07372](https://github.com/denehoffman/gluex-rs/commit/7b07372d9c43537baf51cb71691fabb973bea21f))
* Full lints and precommits plus a Justfile to round it all out ([8e4310d](https://github.com/denehoffman/gluex-rs/commit/8e4310d505b9b31b6037c01452463d66ba38f385))
* **generation:** Add Python-authored standalone event generation ([f1e290e](https://github.com/denehoffman/gluex-rs/commit/f1e290eff358ba51f2739a13430101b41d118b09))
* **generation:** Expose shared Monte Carlo runner to Python ([7c3fdf9](https://github.com/denehoffman/gluex-rs/commit/7c3fdf997ed9d103862cd67c8e221d3420184bd3))
* **gluex-ccdb:** Add CCDB_CONNECTION default constructor and align docs/tests ([025ea46](https://github.com/denehoffman/gluex-rs/commit/025ea46da3edf39419b406628416d2e22dbbd571))
* **gluex-rcdb:** Add RCDB_CONNECTION default constructor and align docs/tests ([00265a3](https://github.com/denehoffman/gluex-rs/commit/00265a39f35ed0674972bbc61702479911b6ca98))
* **histograms.rs:** Add some helper methods to the Histogram class ([ad391c7](https://github.com/denehoffman/gluex-rs/commit/ad391c757e390b7b5bc8160d3e07cf5c631740d3))
* **histograms:** Consolidate histogram errors into a single error type rather than asserts ([8c4ae68](https://github.com/denehoffman/gluex-rs/commit/8c4ae684f572f8422a6555275ff5f284ec0d4614))
* **lumi-py:** Add python bindings and plotting CLI ([3fb3521](https://github.com/denehoffman/gluex-rs/commit/3fb35214a8e190864508f6037dbcd818b2869735))
* Migrate database bindings into unified Python package ([8cb733b](https://github.com/denehoffman/gluex-rs/commit/8cb733b139f902f4c0a0380af5b60f9fecd87607))
* Move CLI ownership to gluex facade ([cf711a6](https://github.com/denehoffman/gluex-rs/commit/cf711a64a895737d61a3b0327965b7db696d6c2c))
* **rcdb:** First draft of RCDB python interface ([a3da761](https://github.com/denehoffman/gluex-rs/commit/a3da761250b62b71221e9f2493f734e172391ed9))
* Release-ready I hope ([aebbf2d](https://github.com/denehoffman/gluex-rs/commit/aebbf2d481f273caaf8987efb55aab72706131a4))
* Restructure crates a bit and add RCDB skeleton crate ([8f1ba69](https://github.com/denehoffman/gluex-rs/commit/8f1ba698b240ac20b2a624d905d8bb820b6a76a6))
* Separate Python crates, add lots of clippy lints, add precommit, and a few other small API changes ([d4de1b6](https://github.com/denehoffman/gluex-rs/commit/d4de1b6a39571d0bc58c769af6514a7c63f49c30))
* Simplify plot argument in Python CLI ([58b8e76](https://github.com/denehoffman/gluex-rs/commit/58b8e76f20e439d79f019d04157b5d348cee16f7))
* Update lumi rest version handling ([15427e5](https://github.com/denehoffman/gluex-rs/commit/15427e523b35966caaecc280f976264f6c16d8a6))
* Update REST version selections, calibration times, and the overall CLI for gluex-lumi to be more informative ([1156773](https://github.com/denehoffman/gluex-rs/commit/1156773210c364ac09f98566a58895ee1b3391b5))


### Bug Fixes

* Add some helper methods to Data/RowView and change accessor function names ([7d9f979](https://github.com/denehoffman/gluex-rs/commit/7d9f979127ed1585bdd310bdb59a7aa17260dfbe))
* Add tests and found flipped column/row arguments in python API ([a8c0460](https://github.com/denehoffman/gluex-rs/commit/a8c046027c6c71a3883cf7a48f9ae3adc025365a))
* Align laddu dependency with generated batch api ([80e0c69](https://github.com/denehoffman/gluex-rs/commit/80e0c696e421c25e943591948723bea6548b8516))
* Bump dependency versions manually since Release-Please doesn't seem to want to ([decf95b](https://github.com/denehoffman/gluex-rs/commit/decf95b259d1d30c0c3d54754e5df858feb8bcbc))
* Bump internal dependencies and allow release-please to bump them in the future ([05faad5](https://github.com/denehoffman/gluex-rs/commit/05faad5afb33cbbe1c081632e2e5d8e5ab529756))
* Bump minimum python version ([1e78225](https://github.com/denehoffman/gluex-rs/commit/1e78225498b69731c9186ecd5bcc30d24fcad166))
* Change cargo layout to hopefully force dependency syncing ([ebb8163](https://github.com/denehoffman/gluex-rs/commit/ebb8163dc40962aea84b88924de0c09e78938d8c))
* Change release-please paths to update dependencies ([fc47fed](https://github.com/denehoffman/gluex-rs/commit/fc47fedecbadd3de3401a57ef9282c019b7fae2a))
* Change timestamp getter names and add comments/descriptions to python ([99c396f](https://github.com/denehoffman/gluex-rs/commit/99c396f74d201ba0e5bff0e26e1bd2752b5bea78))
* Clear ty check ([71a1f0d](https://github.com/denehoffman/gluex-rs/commit/71a1f0d982ceaa48242f8af13fba163918396b11))
* Correct --exclude-runs parsing in CLI ([17776bb](https://github.com/denehoffman/gluex-rs/commit/17776bb7ab3d4c8b1a154582fd5db884728b1c50))
* Correct pytest imports ([b39ab19](https://github.com/denehoffman/gluex-rs/commit/b39ab19284a87f4bd21363b231199f82d78fb602))
* Flatten error types and fix some CI issues ([ff4f71f](https://github.com/denehoffman/gluex-rs/commit/ff4f71fd103bf2741aa36f7205c40d2f5a0c037d))
* **gluex-ccdb-py:** Release gluex-ccdb-py-v0.1.5 ([a0cfa95](https://github.com/denehoffman/gluex-rs/commit/a0cfa95edf2e6e5c43496f9e424783238799c5ec))
* **gluex-ccdb:** Release gluex-ccdb-v0.1.5 ([c7f1245](https://github.com/denehoffman/gluex-rs/commit/c7f124526ee1ccc5cd8b99f8c847e72ed7a42852))
* **gluex-core:** Consolidate error types for gluex-core ([4c69615](https://github.com/denehoffman/gluex-rs/commit/4c6961585f0281ac0dbf2f2cc48b7034dc9842e2))
* **gluex-lumi-py:** Release gluex-lumi-py-v0.1.7 ([c74ab0b](https://github.com/denehoffman/gluex-rs/commit/c74ab0be7254e558bae21149ec025338cfecba6b))
* **gluex-lumi:** Add defaults for bins/min/max in CLI ([4eea103](https://github.com/denehoffman/gluex-rs/commit/4eea1031d4aed2504c9faece60cb4a45bb4549dd))
* **gluex-lumi:** Clippy lints ([123b44a](https://github.com/denehoffman/gluex-rs/commit/123b44ad43401b9e003fb225ec9858d1334a57da))
* **gluex-lumi:** Release gluex-lumi-v0.1.7 ([c61aac8](https://github.com/denehoffman/gluex-rs/commit/c61aac898ed66d068dd13ba7c258c2ae0cedae5a))
* **gluex-rcdb-py:** Release gluex-rcdb-py-v0.1.7 ([6649f15](https://github.com/denehoffman/gluex-rs/commit/6649f15ded268475e8822f303f3319f5802c16de))
* **gluex-rcdb:** Release gluex-rcdb-v0.1.7 ([a4acfb0](https://github.com/denehoffman/gluex-rs/commit/a4acfb026faf8faa6d5b85f4377322a19e5602e8))
* Handle RP2019_11 calibration override ([a330993](https://github.com/denehoffman/gluex-rs/commit/a330993282ee4ce7f3f58bbaae69609c37d1c99a))
* Move fixtures into the proper test folder ([9e9c286](https://github.com/denehoffman/gluex-rs/commit/9e9c2869ddbbfe246414734b07e8015c8fa536d3))
* Update pre-commit and fix tests ([45e087e](https://github.com/denehoffman/gluex-rs/commit/45e087e8e7be4b18694eac74b6fb51024b1c49ab))
* Use laddu's ScalarSource for generation ([8359026](https://github.com/denehoffman/gluex-rs/commit/83590268e3410836d81a510d31a2309b69970544))


### Performance Improvements

* **ccdb:** Speed up column layout reuse and vault parsing ([cc51d3c](https://github.com/denehoffman/gluex-rs/commit/cc51d3c52c6ea21a63e2b6a731b0aa5b22952481))
* **gluex-rcdb:** Benchmark and force run-number index ([a439456](https://github.com/denehoffman/gluex-rs/commit/a43945639bf158c29819eeefe240e0d42df3681f))
* Move temp table creation to database open statement ([e06c6bf](https://github.com/denehoffman/gluex-rs/commit/e06c6bf33ab43c4451db2db0904d0e2371051582))
* Revert from using temp tables to just grabbing all the constant set data when we get assignments ([4c6744c](https://github.com/denehoffman/gluex-rs/commit/4c6744c68a992754b73cfa376b269b11311bcb4b))


### Code Refactoring

* Audit facade APIs for unified bindings ([6ded128](https://github.com/denehoffman/gluex-rs/commit/6ded12890b9f52583a22c243f8cd4aacbdec4ac1))
* **gluex-lumi:** Introduce Context/Luminosity API and update CLI ([396c04f](https://github.com/denehoffman/gluex-rs/commit/396c04f755879de6a627a58481ae8ae492a32eb0))
* Unify workspace into one crate ([3f8e931](https://github.com/denehoffman/gluex-rs/commit/3f8e9316e30f6f18a1649c691672ed2a95d649de))

## Changelog

Release Please maintains this changelog for the unified `gluex-rs` package.
