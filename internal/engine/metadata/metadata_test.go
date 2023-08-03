package metadata

import (
	"strings"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/util"
)

var _ = Describe("Metadata", func() {
	It("should have two column with empty configuration", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		_, err := New(&Config{
			StartAt: startAt,
			EndAt:   endAt,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName: "xx",
			StartAt:   startAt,
			EndAt:     endAt,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).NotTo(BeNil())
		m, err := New(&Config{
			SchemaName:            "public",
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).To(BeNil())
		ddl := m.GetDDL()
		Expect(ddl).To(Equal("\nCREATE EXTENSION IF NOT EXISTS matrixts;\nALTER EXTENSION matrixts UPDATE;\nCREATE SCHEMA IF NOT EXISTS \"public\";\nCREATE TABLE \"public\".\"xx\" (\n\tts timestamp ENCODING (minmax)\n  , vin text ENCODING (minmax)\n)\nUSING mars2 WITH ( compress_threshold='1000', chunk_size='32' )\nDISTRIBUTED BY (vin)\nPARTITION BY RANGE(ts) (\n\tSTART ('2015-12-31 00:00:00')\n\tEND ('2016-01-02 00:00:00')\n\tEVERY ('86400 second'),\n\tDEFAULT PARTITION default_prt\n);\n\nCREATE INDEX IF NOT EXISTS \"idx_xx\" ON \"public\".\"xx\"\nUSING mars2_btree(\n\tvin\n  , ts\n)\nWITH(uniquemode=false);\n"))
	})
	It("should CREATE TABLE with uniquemode set to true", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		_, err := New(&Config{
			StartAt:              startAt,
			EndAt:                endAt,
			HasUniqueConstraints: true,
			StorageType:          StorageMars2,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:            "xx",
			StartAt:              startAt,
			EndAt:                endAt,
			HasUniqueConstraints: true,
			StorageType:          StorageMars2,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			HasUniqueConstraints:  true,
			StorageType:           StorageMars2,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
			HasUniqueConstraints:  true,
			StorageType:           StorageMars2,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
			HasUniqueConstraints:  true,
			StorageType:           StorageMars2,
		})
		Expect(err).NotTo(BeNil())
		m, err := New(&Config{
			SchemaName:            "public",
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			MetricsType:           MetricsTypeFloat4,
			HasUniqueConstraints:  true,
			StorageType:           StorageMars2,
		})
		Expect(err).To(BeNil())
		ddl := m.GetDDL()
		ddlExpect := `
CREATE EXTENSION IF NOT EXISTS matrixts;
ALTER EXTENSION matrixts UPDATE;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE "public"."xx" (
	ts timestamp ENCODING (minmax)
  , vin text ENCODING (minmax)
)
USING mars2 WITH ( compress_threshold='1000', chunk_size='32' )
DISTRIBUTED BY (vin)

PARTITION BY RANGE(ts) (
	START ('2015-12-31 00:00:00')
	END ('2016-01-02 00:00:00')
	EVERY ('86400 second'),
	DEFAULT PARTITION default_prt
);

CREATE INDEX IF NOT EXISTS "idx_xx" ON "public"."xx"
USING mars2_btree(
	vin
  , ts
)
WITH(uniquemode=true);
`
		Expect(ddl).To(Equal(ddlExpect))
	})

	It("should CREATE TABLE without json column", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		_, err := New(&Config{
			StartAt: startAt,
			EndAt:   endAt,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName: "xx",
			StartAt:   startAt,
			EndAt:     endAt,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			TotalMetricsCount:     998,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).NotTo(BeNil())
		_, err = New(&Config{
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			TotalMetricsCount:     998,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).NotTo(BeNil())
		m, err := New(&Config{
			SchemaName:            "public",
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			TotalMetricsCount:     998,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).To(BeNil())
		ddl := m.GetDDL()
		Expect(ddl).To(Equal(`
CREATE EXTENSION IF NOT EXISTS matrixts;
ALTER EXTENSION matrixts UPDATE;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE "public"."xx" (
	ts timestamp ENCODING (minmax)
  , vin text ENCODING (minmax)
  , c0 float4
  , c1 float4
  , c2 float4
  , c3 float4
  , c4 float4
  , c5 float4
  , c6 float4
  , c7 float4
  , c8 float4
  , c9 float4
  , c10 float4
  , c11 float4
  , c12 float4
  , c13 float4
  , c14 float4
  , c15 float4
  , c16 float4
  , c17 float4
  , c18 float4
  , c19 float4
  , c20 float4
  , c21 float4
  , c22 float4
  , c23 float4
  , c24 float4
  , c25 float4
  , c26 float4
  , c27 float4
  , c28 float4
  , c29 float4
  , c30 float4
  , c31 float4
  , c32 float4
  , c33 float4
  , c34 float4
  , c35 float4
  , c36 float4
  , c37 float4
  , c38 float4
  , c39 float4
  , c40 float4
  , c41 float4
  , c42 float4
  , c43 float4
  , c44 float4
  , c45 float4
  , c46 float4
  , c47 float4
  , c48 float4
  , c49 float4
  , c50 float4
  , c51 float4
  , c52 float4
  , c53 float4
  , c54 float4
  , c55 float4
  , c56 float4
  , c57 float4
  , c58 float4
  , c59 float4
  , c60 float4
  , c61 float4
  , c62 float4
  , c63 float4
  , c64 float4
  , c65 float4
  , c66 float4
  , c67 float4
  , c68 float4
  , c69 float4
  , c70 float4
  , c71 float4
  , c72 float4
  , c73 float4
  , c74 float4
  , c75 float4
  , c76 float4
  , c77 float4
  , c78 float4
  , c79 float4
  , c80 float4
  , c81 float4
  , c82 float4
  , c83 float4
  , c84 float4
  , c85 float4
  , c86 float4
  , c87 float4
  , c88 float4
  , c89 float4
  , c90 float4
  , c91 float4
  , c92 float4
  , c93 float4
  , c94 float4
  , c95 float4
  , c96 float4
  , c97 float4
  , c98 float4
  , c99 float4
  , c100 float4
  , c101 float4
  , c102 float4
  , c103 float4
  , c104 float4
  , c105 float4
  , c106 float4
  , c107 float4
  , c108 float4
  , c109 float4
  , c110 float4
  , c111 float4
  , c112 float4
  , c113 float4
  , c114 float4
  , c115 float4
  , c116 float4
  , c117 float4
  , c118 float4
  , c119 float4
  , c120 float4
  , c121 float4
  , c122 float4
  , c123 float4
  , c124 float4
  , c125 float4
  , c126 float4
  , c127 float4
  , c128 float4
  , c129 float4
  , c130 float4
  , c131 float4
  , c132 float4
  , c133 float4
  , c134 float4
  , c135 float4
  , c136 float4
  , c137 float4
  , c138 float4
  , c139 float4
  , c140 float4
  , c141 float4
  , c142 float4
  , c143 float4
  , c144 float4
  , c145 float4
  , c146 float4
  , c147 float4
  , c148 float4
  , c149 float4
  , c150 float4
  , c151 float4
  , c152 float4
  , c153 float4
  , c154 float4
  , c155 float4
  , c156 float4
  , c157 float4
  , c158 float4
  , c159 float4
  , c160 float4
  , c161 float4
  , c162 float4
  , c163 float4
  , c164 float4
  , c165 float4
  , c166 float4
  , c167 float4
  , c168 float4
  , c169 float4
  , c170 float4
  , c171 float4
  , c172 float4
  , c173 float4
  , c174 float4
  , c175 float4
  , c176 float4
  , c177 float4
  , c178 float4
  , c179 float4
  , c180 float4
  , c181 float4
  , c182 float4
  , c183 float4
  , c184 float4
  , c185 float4
  , c186 float4
  , c187 float4
  , c188 float4
  , c189 float4
  , c190 float4
  , c191 float4
  , c192 float4
  , c193 float4
  , c194 float4
  , c195 float4
  , c196 float4
  , c197 float4
  , c198 float4
  , c199 float4
  , c200 float4
  , c201 float4
  , c202 float4
  , c203 float4
  , c204 float4
  , c205 float4
  , c206 float4
  , c207 float4
  , c208 float4
  , c209 float4
  , c210 float4
  , c211 float4
  , c212 float4
  , c213 float4
  , c214 float4
  , c215 float4
  , c216 float4
  , c217 float4
  , c218 float4
  , c219 float4
  , c220 float4
  , c221 float4
  , c222 float4
  , c223 float4
  , c224 float4
  , c225 float4
  , c226 float4
  , c227 float4
  , c228 float4
  , c229 float4
  , c230 float4
  , c231 float4
  , c232 float4
  , c233 float4
  , c234 float4
  , c235 float4
  , c236 float4
  , c237 float4
  , c238 float4
  , c239 float4
  , c240 float4
  , c241 float4
  , c242 float4
  , c243 float4
  , c244 float4
  , c245 float4
  , c246 float4
  , c247 float4
  , c248 float4
  , c249 float4
  , c250 float4
  , c251 float4
  , c252 float4
  , c253 float4
  , c254 float4
  , c255 float4
  , c256 float4
  , c257 float4
  , c258 float4
  , c259 float4
  , c260 float4
  , c261 float4
  , c262 float4
  , c263 float4
  , c264 float4
  , c265 float4
  , c266 float4
  , c267 float4
  , c268 float4
  , c269 float4
  , c270 float4
  , c271 float4
  , c272 float4
  , c273 float4
  , c274 float4
  , c275 float4
  , c276 float4
  , c277 float4
  , c278 float4
  , c279 float4
  , c280 float4
  , c281 float4
  , c282 float4
  , c283 float4
  , c284 float4
  , c285 float4
  , c286 float4
  , c287 float4
  , c288 float4
  , c289 float4
  , c290 float4
  , c291 float4
  , c292 float4
  , c293 float4
  , c294 float4
  , c295 float4
  , c296 float4
  , c297 float4
  , c298 float4
  , c299 float4
  , c300 float4
  , c301 float4
  , c302 float4
  , c303 float4
  , c304 float4
  , c305 float4
  , c306 float4
  , c307 float4
  , c308 float4
  , c309 float4
  , c310 float4
  , c311 float4
  , c312 float4
  , c313 float4
  , c314 float4
  , c315 float4
  , c316 float4
  , c317 float4
  , c318 float4
  , c319 float4
  , c320 float4
  , c321 float4
  , c322 float4
  , c323 float4
  , c324 float4
  , c325 float4
  , c326 float4
  , c327 float4
  , c328 float4
  , c329 float4
  , c330 float4
  , c331 float4
  , c332 float4
  , c333 float4
  , c334 float4
  , c335 float4
  , c336 float4
  , c337 float4
  , c338 float4
  , c339 float4
  , c340 float4
  , c341 float4
  , c342 float4
  , c343 float4
  , c344 float4
  , c345 float4
  , c346 float4
  , c347 float4
  , c348 float4
  , c349 float4
  , c350 float4
  , c351 float4
  , c352 float4
  , c353 float4
  , c354 float4
  , c355 float4
  , c356 float4
  , c357 float4
  , c358 float4
  , c359 float4
  , c360 float4
  , c361 float4
  , c362 float4
  , c363 float4
  , c364 float4
  , c365 float4
  , c366 float4
  , c367 float4
  , c368 float4
  , c369 float4
  , c370 float4
  , c371 float4
  , c372 float4
  , c373 float4
  , c374 float4
  , c375 float4
  , c376 float4
  , c377 float4
  , c378 float4
  , c379 float4
  , c380 float4
  , c381 float4
  , c382 float4
  , c383 float4
  , c384 float4
  , c385 float4
  , c386 float4
  , c387 float4
  , c388 float4
  , c389 float4
  , c390 float4
  , c391 float4
  , c392 float4
  , c393 float4
  , c394 float4
  , c395 float4
  , c396 float4
  , c397 float4
  , c398 float4
  , c399 float4
  , c400 float4
  , c401 float4
  , c402 float4
  , c403 float4
  , c404 float4
  , c405 float4
  , c406 float4
  , c407 float4
  , c408 float4
  , c409 float4
  , c410 float4
  , c411 float4
  , c412 float4
  , c413 float4
  , c414 float4
  , c415 float4
  , c416 float4
  , c417 float4
  , c418 float4
  , c419 float4
  , c420 float4
  , c421 float4
  , c422 float4
  , c423 float4
  , c424 float4
  , c425 float4
  , c426 float4
  , c427 float4
  , c428 float4
  , c429 float4
  , c430 float4
  , c431 float4
  , c432 float4
  , c433 float4
  , c434 float4
  , c435 float4
  , c436 float4
  , c437 float4
  , c438 float4
  , c439 float4
  , c440 float4
  , c441 float4
  , c442 float4
  , c443 float4
  , c444 float4
  , c445 float4
  , c446 float4
  , c447 float4
  , c448 float4
  , c449 float4
  , c450 float4
  , c451 float4
  , c452 float4
  , c453 float4
  , c454 float4
  , c455 float4
  , c456 float4
  , c457 float4
  , c458 float4
  , c459 float4
  , c460 float4
  , c461 float4
  , c462 float4
  , c463 float4
  , c464 float4
  , c465 float4
  , c466 float4
  , c467 float4
  , c468 float4
  , c469 float4
  , c470 float4
  , c471 float4
  , c472 float4
  , c473 float4
  , c474 float4
  , c475 float4
  , c476 float4
  , c477 float4
  , c478 float4
  , c479 float4
  , c480 float4
  , c481 float4
  , c482 float4
  , c483 float4
  , c484 float4
  , c485 float4
  , c486 float4
  , c487 float4
  , c488 float4
  , c489 float4
  , c490 float4
  , c491 float4
  , c492 float4
  , c493 float4
  , c494 float4
  , c495 float4
  , c496 float4
  , c497 float4
  , c498 float4
  , c499 float4
  , c500 float4
  , c501 float4
  , c502 float4
  , c503 float4
  , c504 float4
  , c505 float4
  , c506 float4
  , c507 float4
  , c508 float4
  , c509 float4
  , c510 float4
  , c511 float4
  , c512 float4
  , c513 float4
  , c514 float4
  , c515 float4
  , c516 float4
  , c517 float4
  , c518 float4
  , c519 float4
  , c520 float4
  , c521 float4
  , c522 float4
  , c523 float4
  , c524 float4
  , c525 float4
  , c526 float4
  , c527 float4
  , c528 float4
  , c529 float4
  , c530 float4
  , c531 float4
  , c532 float4
  , c533 float4
  , c534 float4
  , c535 float4
  , c536 float4
  , c537 float4
  , c538 float4
  , c539 float4
  , c540 float4
  , c541 float4
  , c542 float4
  , c543 float4
  , c544 float4
  , c545 float4
  , c546 float4
  , c547 float4
  , c548 float4
  , c549 float4
  , c550 float4
  , c551 float4
  , c552 float4
  , c553 float4
  , c554 float4
  , c555 float4
  , c556 float4
  , c557 float4
  , c558 float4
  , c559 float4
  , c560 float4
  , c561 float4
  , c562 float4
  , c563 float4
  , c564 float4
  , c565 float4
  , c566 float4
  , c567 float4
  , c568 float4
  , c569 float4
  , c570 float4
  , c571 float4
  , c572 float4
  , c573 float4
  , c574 float4
  , c575 float4
  , c576 float4
  , c577 float4
  , c578 float4
  , c579 float4
  , c580 float4
  , c581 float4
  , c582 float4
  , c583 float4
  , c584 float4
  , c585 float4
  , c586 float4
  , c587 float4
  , c588 float4
  , c589 float4
  , c590 float4
  , c591 float4
  , c592 float4
  , c593 float4
  , c594 float4
  , c595 float4
  , c596 float4
  , c597 float4
  , c598 float4
  , c599 float4
  , c600 float4
  , c601 float4
  , c602 float4
  , c603 float4
  , c604 float4
  , c605 float4
  , c606 float4
  , c607 float4
  , c608 float4
  , c609 float4
  , c610 float4
  , c611 float4
  , c612 float4
  , c613 float4
  , c614 float4
  , c615 float4
  , c616 float4
  , c617 float4
  , c618 float4
  , c619 float4
  , c620 float4
  , c621 float4
  , c622 float4
  , c623 float4
  , c624 float4
  , c625 float4
  , c626 float4
  , c627 float4
  , c628 float4
  , c629 float4
  , c630 float4
  , c631 float4
  , c632 float4
  , c633 float4
  , c634 float4
  , c635 float4
  , c636 float4
  , c637 float4
  , c638 float4
  , c639 float4
  , c640 float4
  , c641 float4
  , c642 float4
  , c643 float4
  , c644 float4
  , c645 float4
  , c646 float4
  , c647 float4
  , c648 float4
  , c649 float4
  , c650 float4
  , c651 float4
  , c652 float4
  , c653 float4
  , c654 float4
  , c655 float4
  , c656 float4
  , c657 float4
  , c658 float4
  , c659 float4
  , c660 float4
  , c661 float4
  , c662 float4
  , c663 float4
  , c664 float4
  , c665 float4
  , c666 float4
  , c667 float4
  , c668 float4
  , c669 float4
  , c670 float4
  , c671 float4
  , c672 float4
  , c673 float4
  , c674 float4
  , c675 float4
  , c676 float4
  , c677 float4
  , c678 float4
  , c679 float4
  , c680 float4
  , c681 float4
  , c682 float4
  , c683 float4
  , c684 float4
  , c685 float4
  , c686 float4
  , c687 float4
  , c688 float4
  , c689 float4
  , c690 float4
  , c691 float4
  , c692 float4
  , c693 float4
  , c694 float4
  , c695 float4
  , c696 float4
  , c697 float4
  , c698 float4
  , c699 float4
  , c700 float4
  , c701 float4
  , c702 float4
  , c703 float4
  , c704 float4
  , c705 float4
  , c706 float4
  , c707 float4
  , c708 float4
  , c709 float4
  , c710 float4
  , c711 float4
  , c712 float4
  , c713 float4
  , c714 float4
  , c715 float4
  , c716 float4
  , c717 float4
  , c718 float4
  , c719 float4
  , c720 float4
  , c721 float4
  , c722 float4
  , c723 float4
  , c724 float4
  , c725 float4
  , c726 float4
  , c727 float4
  , c728 float4
  , c729 float4
  , c730 float4
  , c731 float4
  , c732 float4
  , c733 float4
  , c734 float4
  , c735 float4
  , c736 float4
  , c737 float4
  , c738 float4
  , c739 float4
  , c740 float4
  , c741 float4
  , c742 float4
  , c743 float4
  , c744 float4
  , c745 float4
  , c746 float4
  , c747 float4
  , c748 float4
  , c749 float4
  , c750 float4
  , c751 float4
  , c752 float4
  , c753 float4
  , c754 float4
  , c755 float4
  , c756 float4
  , c757 float4
  , c758 float4
  , c759 float4
  , c760 float4
  , c761 float4
  , c762 float4
  , c763 float4
  , c764 float4
  , c765 float4
  , c766 float4
  , c767 float4
  , c768 float4
  , c769 float4
  , c770 float4
  , c771 float4
  , c772 float4
  , c773 float4
  , c774 float4
  , c775 float4
  , c776 float4
  , c777 float4
  , c778 float4
  , c779 float4
  , c780 float4
  , c781 float4
  , c782 float4
  , c783 float4
  , c784 float4
  , c785 float4
  , c786 float4
  , c787 float4
  , c788 float4
  , c789 float4
  , c790 float4
  , c791 float4
  , c792 float4
  , c793 float4
  , c794 float4
  , c795 float4
  , c796 float4
  , c797 float4
  , c798 float4
  , c799 float4
  , c800 float4
  , c801 float4
  , c802 float4
  , c803 float4
  , c804 float4
  , c805 float4
  , c806 float4
  , c807 float4
  , c808 float4
  , c809 float4
  , c810 float4
  , c811 float4
  , c812 float4
  , c813 float4
  , c814 float4
  , c815 float4
  , c816 float4
  , c817 float4
  , c818 float4
  , c819 float4
  , c820 float4
  , c821 float4
  , c822 float4
  , c823 float4
  , c824 float4
  , c825 float4
  , c826 float4
  , c827 float4
  , c828 float4
  , c829 float4
  , c830 float4
  , c831 float4
  , c832 float4
  , c833 float4
  , c834 float4
  , c835 float4
  , c836 float4
  , c837 float4
  , c838 float4
  , c839 float4
  , c840 float4
  , c841 float4
  , c842 float4
  , c843 float4
  , c844 float4
  , c845 float4
  , c846 float4
  , c847 float4
  , c848 float4
  , c849 float4
  , c850 float4
  , c851 float4
  , c852 float4
  , c853 float4
  , c854 float4
  , c855 float4
  , c856 float4
  , c857 float4
  , c858 float4
  , c859 float4
  , c860 float4
  , c861 float4
  , c862 float4
  , c863 float4
  , c864 float4
  , c865 float4
  , c866 float4
  , c867 float4
  , c868 float4
  , c869 float4
  , c870 float4
  , c871 float4
  , c872 float4
  , c873 float4
  , c874 float4
  , c875 float4
  , c876 float4
  , c877 float4
  , c878 float4
  , c879 float4
  , c880 float4
  , c881 float4
  , c882 float4
  , c883 float4
  , c884 float4
  , c885 float4
  , c886 float4
  , c887 float4
  , c888 float4
  , c889 float4
  , c890 float4
  , c891 float4
  , c892 float4
  , c893 float4
  , c894 float4
  , c895 float4
  , c896 float4
  , c897 float4
  , c898 float4
  , c899 float4
  , c900 float4
  , c901 float4
  , c902 float4
  , c903 float4
  , c904 float4
  , c905 float4
  , c906 float4
  , c907 float4
  , c908 float4
  , c909 float4
  , c910 float4
  , c911 float4
  , c912 float4
  , c913 float4
  , c914 float4
  , c915 float4
  , c916 float4
  , c917 float4
  , c918 float4
  , c919 float4
  , c920 float4
  , c921 float4
  , c922 float4
  , c923 float4
  , c924 float4
  , c925 float4
  , c926 float4
  , c927 float4
  , c928 float4
  , c929 float4
  , c930 float4
  , c931 float4
  , c932 float4
  , c933 float4
  , c934 float4
  , c935 float4
  , c936 float4
  , c937 float4
  , c938 float4
  , c939 float4
  , c940 float4
  , c941 float4
  , c942 float4
  , c943 float4
  , c944 float4
  , c945 float4
  , c946 float4
  , c947 float4
  , c948 float4
  , c949 float4
  , c950 float4
  , c951 float4
  , c952 float4
  , c953 float4
  , c954 float4
  , c955 float4
  , c956 float4
  , c957 float4
  , c958 float4
  , c959 float4
  , c960 float4
  , c961 float4
  , c962 float4
  , c963 float4
  , c964 float4
  , c965 float4
  , c966 float4
  , c967 float4
  , c968 float4
  , c969 float4
  , c970 float4
  , c971 float4
  , c972 float4
  , c973 float4
  , c974 float4
  , c975 float4
  , c976 float4
  , c977 float4
  , c978 float4
  , c979 float4
  , c980 float4
  , c981 float4
  , c982 float4
  , c983 float4
  , c984 float4
  , c985 float4
  , c986 float4
  , c987 float4
  , c988 float4
  , c989 float4
  , c990 float4
  , c991 float4
  , c992 float4
  , c993 float4
  , c994 float4
  , c995 float4
  , c996 float4
  , c997 float4
)
USING mars2 WITH ( compress_threshold='1000', chunk_size='32' )
DISTRIBUTED BY (vin)
PARTITION BY RANGE(ts) (
	START ('2015-12-31 00:00:00')
	END ('2016-01-02 00:00:00')
	EVERY ('86400 second'),
	DEFAULT PARTITION default_prt
);

CREATE INDEX IF NOT EXISTS "idx_xx" ON "public"."xx"
USING mars2_btree(
	vin
  , ts
)
WITH(uniquemode=false);
`))
	})

	It("should CREATE TABLE with json column", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")

		m, err := New(&Config{
			SchemaName:            "public",
			TableName:             "xx",
			TagNum:                3125,
			StartAt:               startAt,
			EndAt:                 endAt,
			TimestampStepInSecond: 1,
			TotalMetricsCount:     999,
			MetricsType:           MetricsTypeFloat4,
		})
		Expect(err).To(BeNil())
		ddl := m.GetDDL()
		Expect(ddl).To(Equal(`
CREATE EXTENSION IF NOT EXISTS matrixts;
ALTER EXTENSION matrixts UPDATE;
CREATE SCHEMA IF NOT EXISTS "public";
CREATE TABLE "public"."xx" (
	ts timestamp ENCODING (minmax)
  , vin text ENCODING (minmax)
  , c0 float4
  , c1 float4
  , c2 float4
  , c3 float4
  , c4 float4
  , c5 float4
  , c6 float4
  , c7 float4
  , c8 float4
  , c9 float4
  , c10 float4
  , c11 float4
  , c12 float4
  , c13 float4
  , c14 float4
  , c15 float4
  , c16 float4
  , c17 float4
  , c18 float4
  , c19 float4
  , c20 float4
  , c21 float4
  , c22 float4
  , c23 float4
  , c24 float4
  , c25 float4
  , c26 float4
  , c27 float4
  , c28 float4
  , c29 float4
  , c30 float4
  , c31 float4
  , c32 float4
  , c33 float4
  , c34 float4
  , c35 float4
  , c36 float4
  , c37 float4
  , c38 float4
  , c39 float4
  , c40 float4
  , c41 float4
  , c42 float4
  , c43 float4
  , c44 float4
  , c45 float4
  , c46 float4
  , c47 float4
  , c48 float4
  , c49 float4
  , c50 float4
  , c51 float4
  , c52 float4
  , c53 float4
  , c54 float4
  , c55 float4
  , c56 float4
  , c57 float4
  , c58 float4
  , c59 float4
  , c60 float4
  , c61 float4
  , c62 float4
  , c63 float4
  , c64 float4
  , c65 float4
  , c66 float4
  , c67 float4
  , c68 float4
  , c69 float4
  , c70 float4
  , c71 float4
  , c72 float4
  , c73 float4
  , c74 float4
  , c75 float4
  , c76 float4
  , c77 float4
  , c78 float4
  , c79 float4
  , c80 float4
  , c81 float4
  , c82 float4
  , c83 float4
  , c84 float4
  , c85 float4
  , c86 float4
  , c87 float4
  , c88 float4
  , c89 float4
  , c90 float4
  , c91 float4
  , c92 float4
  , c93 float4
  , c94 float4
  , c95 float4
  , c96 float4
  , c97 float4
  , c98 float4
  , c99 float4
  , c100 float4
  , c101 float4
  , c102 float4
  , c103 float4
  , c104 float4
  , c105 float4
  , c106 float4
  , c107 float4
  , c108 float4
  , c109 float4
  , c110 float4
  , c111 float4
  , c112 float4
  , c113 float4
  , c114 float4
  , c115 float4
  , c116 float4
  , c117 float4
  , c118 float4
  , c119 float4
  , c120 float4
  , c121 float4
  , c122 float4
  , c123 float4
  , c124 float4
  , c125 float4
  , c126 float4
  , c127 float4
  , c128 float4
  , c129 float4
  , c130 float4
  , c131 float4
  , c132 float4
  , c133 float4
  , c134 float4
  , c135 float4
  , c136 float4
  , c137 float4
  , c138 float4
  , c139 float4
  , c140 float4
  , c141 float4
  , c142 float4
  , c143 float4
  , c144 float4
  , c145 float4
  , c146 float4
  , c147 float4
  , c148 float4
  , c149 float4
  , c150 float4
  , c151 float4
  , c152 float4
  , c153 float4
  , c154 float4
  , c155 float4
  , c156 float4
  , c157 float4
  , c158 float4
  , c159 float4
  , c160 float4
  , c161 float4
  , c162 float4
  , c163 float4
  , c164 float4
  , c165 float4
  , c166 float4
  , c167 float4
  , c168 float4
  , c169 float4
  , c170 float4
  , c171 float4
  , c172 float4
  , c173 float4
  , c174 float4
  , c175 float4
  , c176 float4
  , c177 float4
  , c178 float4
  , c179 float4
  , c180 float4
  , c181 float4
  , c182 float4
  , c183 float4
  , c184 float4
  , c185 float4
  , c186 float4
  , c187 float4
  , c188 float4
  , c189 float4
  , c190 float4
  , c191 float4
  , c192 float4
  , c193 float4
  , c194 float4
  , c195 float4
  , c196 float4
  , c197 float4
  , c198 float4
  , c199 float4
  , c200 float4
  , c201 float4
  , c202 float4
  , c203 float4
  , c204 float4
  , c205 float4
  , c206 float4
  , c207 float4
  , c208 float4
  , c209 float4
  , c210 float4
  , c211 float4
  , c212 float4
  , c213 float4
  , c214 float4
  , c215 float4
  , c216 float4
  , c217 float4
  , c218 float4
  , c219 float4
  , c220 float4
  , c221 float4
  , c222 float4
  , c223 float4
  , c224 float4
  , c225 float4
  , c226 float4
  , c227 float4
  , c228 float4
  , c229 float4
  , c230 float4
  , c231 float4
  , c232 float4
  , c233 float4
  , c234 float4
  , c235 float4
  , c236 float4
  , c237 float4
  , c238 float4
  , c239 float4
  , c240 float4
  , c241 float4
  , c242 float4
  , c243 float4
  , c244 float4
  , c245 float4
  , c246 float4
  , c247 float4
  , c248 float4
  , c249 float4
  , c250 float4
  , c251 float4
  , c252 float4
  , c253 float4
  , c254 float4
  , c255 float4
  , c256 float4
  , c257 float4
  , c258 float4
  , c259 float4
  , c260 float4
  , c261 float4
  , c262 float4
  , c263 float4
  , c264 float4
  , c265 float4
  , c266 float4
  , c267 float4
  , c268 float4
  , c269 float4
  , c270 float4
  , c271 float4
  , c272 float4
  , c273 float4
  , c274 float4
  , c275 float4
  , c276 float4
  , c277 float4
  , c278 float4
  , c279 float4
  , c280 float4
  , c281 float4
  , c282 float4
  , c283 float4
  , c284 float4
  , c285 float4
  , c286 float4
  , c287 float4
  , c288 float4
  , c289 float4
  , c290 float4
  , c291 float4
  , c292 float4
  , c293 float4
  , c294 float4
  , c295 float4
  , c296 float4
  , c297 float4
  , c298 float4
  , c299 float4
  , c300 float4
  , c301 float4
  , c302 float4
  , c303 float4
  , c304 float4
  , c305 float4
  , c306 float4
  , c307 float4
  , c308 float4
  , c309 float4
  , c310 float4
  , c311 float4
  , c312 float4
  , c313 float4
  , c314 float4
  , c315 float4
  , c316 float4
  , c317 float4
  , c318 float4
  , c319 float4
  , c320 float4
  , c321 float4
  , c322 float4
  , c323 float4
  , c324 float4
  , c325 float4
  , c326 float4
  , c327 float4
  , c328 float4
  , c329 float4
  , c330 float4
  , c331 float4
  , c332 float4
  , c333 float4
  , c334 float4
  , c335 float4
  , c336 float4
  , c337 float4
  , c338 float4
  , c339 float4
  , c340 float4
  , c341 float4
  , c342 float4
  , c343 float4
  , c344 float4
  , c345 float4
  , c346 float4
  , c347 float4
  , c348 float4
  , c349 float4
  , c350 float4
  , c351 float4
  , c352 float4
  , c353 float4
  , c354 float4
  , c355 float4
  , c356 float4
  , c357 float4
  , c358 float4
  , c359 float4
  , c360 float4
  , c361 float4
  , c362 float4
  , c363 float4
  , c364 float4
  , c365 float4
  , c366 float4
  , c367 float4
  , c368 float4
  , c369 float4
  , c370 float4
  , c371 float4
  , c372 float4
  , c373 float4
  , c374 float4
  , c375 float4
  , c376 float4
  , c377 float4
  , c378 float4
  , c379 float4
  , c380 float4
  , c381 float4
  , c382 float4
  , c383 float4
  , c384 float4
  , c385 float4
  , c386 float4
  , c387 float4
  , c388 float4
  , c389 float4
  , c390 float4
  , c391 float4
  , c392 float4
  , c393 float4
  , c394 float4
  , c395 float4
  , c396 float4
  , c397 float4
  , c398 float4
  , c399 float4
  , c400 float4
  , c401 float4
  , c402 float4
  , c403 float4
  , c404 float4
  , c405 float4
  , c406 float4
  , c407 float4
  , c408 float4
  , c409 float4
  , c410 float4
  , c411 float4
  , c412 float4
  , c413 float4
  , c414 float4
  , c415 float4
  , c416 float4
  , c417 float4
  , c418 float4
  , c419 float4
  , c420 float4
  , c421 float4
  , c422 float4
  , c423 float4
  , c424 float4
  , c425 float4
  , c426 float4
  , c427 float4
  , c428 float4
  , c429 float4
  , c430 float4
  , c431 float4
  , c432 float4
  , c433 float4
  , c434 float4
  , c435 float4
  , c436 float4
  , c437 float4
  , c438 float4
  , c439 float4
  , c440 float4
  , c441 float4
  , c442 float4
  , c443 float4
  , c444 float4
  , c445 float4
  , c446 float4
  , c447 float4
  , c448 float4
  , c449 float4
  , c450 float4
  , c451 float4
  , c452 float4
  , c453 float4
  , c454 float4
  , c455 float4
  , c456 float4
  , c457 float4
  , c458 float4
  , c459 float4
  , c460 float4
  , c461 float4
  , c462 float4
  , c463 float4
  , c464 float4
  , c465 float4
  , c466 float4
  , c467 float4
  , c468 float4
  , c469 float4
  , c470 float4
  , c471 float4
  , c472 float4
  , c473 float4
  , c474 float4
  , c475 float4
  , c476 float4
  , c477 float4
  , c478 float4
  , c479 float4
  , c480 float4
  , c481 float4
  , c482 float4
  , c483 float4
  , c484 float4
  , c485 float4
  , c486 float4
  , c487 float4
  , c488 float4
  , c489 float4
  , c490 float4
  , c491 float4
  , c492 float4
  , c493 float4
  , c494 float4
  , c495 float4
  , c496 float4
  , c497 float4
  , c498 float4
  , c499 float4
  , c500 float4
  , c501 float4
  , c502 float4
  , c503 float4
  , c504 float4
  , c505 float4
  , c506 float4
  , c507 float4
  , c508 float4
  , c509 float4
  , c510 float4
  , c511 float4
  , c512 float4
  , c513 float4
  , c514 float4
  , c515 float4
  , c516 float4
  , c517 float4
  , c518 float4
  , c519 float4
  , c520 float4
  , c521 float4
  , c522 float4
  , c523 float4
  , c524 float4
  , c525 float4
  , c526 float4
  , c527 float4
  , c528 float4
  , c529 float4
  , c530 float4
  , c531 float4
  , c532 float4
  , c533 float4
  , c534 float4
  , c535 float4
  , c536 float4
  , c537 float4
  , c538 float4
  , c539 float4
  , c540 float4
  , c541 float4
  , c542 float4
  , c543 float4
  , c544 float4
  , c545 float4
  , c546 float4
  , c547 float4
  , c548 float4
  , c549 float4
  , c550 float4
  , c551 float4
  , c552 float4
  , c553 float4
  , c554 float4
  , c555 float4
  , c556 float4
  , c557 float4
  , c558 float4
  , c559 float4
  , c560 float4
  , c561 float4
  , c562 float4
  , c563 float4
  , c564 float4
  , c565 float4
  , c566 float4
  , c567 float4
  , c568 float4
  , c569 float4
  , c570 float4
  , c571 float4
  , c572 float4
  , c573 float4
  , c574 float4
  , c575 float4
  , c576 float4
  , c577 float4
  , c578 float4
  , c579 float4
  , c580 float4
  , c581 float4
  , c582 float4
  , c583 float4
  , c584 float4
  , c585 float4
  , c586 float4
  , c587 float4
  , c588 float4
  , c589 float4
  , c590 float4
  , c591 float4
  , c592 float4
  , c593 float4
  , c594 float4
  , c595 float4
  , c596 float4
  , c597 float4
  , c598 float4
  , c599 float4
  , c600 float4
  , c601 float4
  , c602 float4
  , c603 float4
  , c604 float4
  , c605 float4
  , c606 float4
  , c607 float4
  , c608 float4
  , c609 float4
  , c610 float4
  , c611 float4
  , c612 float4
  , c613 float4
  , c614 float4
  , c615 float4
  , c616 float4
  , c617 float4
  , c618 float4
  , c619 float4
  , c620 float4
  , c621 float4
  , c622 float4
  , c623 float4
  , c624 float4
  , c625 float4
  , c626 float4
  , c627 float4
  , c628 float4
  , c629 float4
  , c630 float4
  , c631 float4
  , c632 float4
  , c633 float4
  , c634 float4
  , c635 float4
  , c636 float4
  , c637 float4
  , c638 float4
  , c639 float4
  , c640 float4
  , c641 float4
  , c642 float4
  , c643 float4
  , c644 float4
  , c645 float4
  , c646 float4
  , c647 float4
  , c648 float4
  , c649 float4
  , c650 float4
  , c651 float4
  , c652 float4
  , c653 float4
  , c654 float4
  , c655 float4
  , c656 float4
  , c657 float4
  , c658 float4
  , c659 float4
  , c660 float4
  , c661 float4
  , c662 float4
  , c663 float4
  , c664 float4
  , c665 float4
  , c666 float4
  , c667 float4
  , c668 float4
  , c669 float4
  , c670 float4
  , c671 float4
  , c672 float4
  , c673 float4
  , c674 float4
  , c675 float4
  , c676 float4
  , c677 float4
  , c678 float4
  , c679 float4
  , c680 float4
  , c681 float4
  , c682 float4
  , c683 float4
  , c684 float4
  , c685 float4
  , c686 float4
  , c687 float4
  , c688 float4
  , c689 float4
  , c690 float4
  , c691 float4
  , c692 float4
  , c693 float4
  , c694 float4
  , c695 float4
  , c696 float4
  , c697 float4
  , c698 float4
  , c699 float4
  , c700 float4
  , c701 float4
  , c702 float4
  , c703 float4
  , c704 float4
  , c705 float4
  , c706 float4
  , c707 float4
  , c708 float4
  , c709 float4
  , c710 float4
  , c711 float4
  , c712 float4
  , c713 float4
  , c714 float4
  , c715 float4
  , c716 float4
  , c717 float4
  , c718 float4
  , c719 float4
  , c720 float4
  , c721 float4
  , c722 float4
  , c723 float4
  , c724 float4
  , c725 float4
  , c726 float4
  , c727 float4
  , c728 float4
  , c729 float4
  , c730 float4
  , c731 float4
  , c732 float4
  , c733 float4
  , c734 float4
  , c735 float4
  , c736 float4
  , c737 float4
  , c738 float4
  , c739 float4
  , c740 float4
  , c741 float4
  , c742 float4
  , c743 float4
  , c744 float4
  , c745 float4
  , c746 float4
  , c747 float4
  , c748 float4
  , c749 float4
  , c750 float4
  , c751 float4
  , c752 float4
  , c753 float4
  , c754 float4
  , c755 float4
  , c756 float4
  , c757 float4
  , c758 float4
  , c759 float4
  , c760 float4
  , c761 float4
  , c762 float4
  , c763 float4
  , c764 float4
  , c765 float4
  , c766 float4
  , c767 float4
  , c768 float4
  , c769 float4
  , c770 float4
  , c771 float4
  , c772 float4
  , c773 float4
  , c774 float4
  , c775 float4
  , c776 float4
  , c777 float4
  , c778 float4
  , c779 float4
  , c780 float4
  , c781 float4
  , c782 float4
  , c783 float4
  , c784 float4
  , c785 float4
  , c786 float4
  , c787 float4
  , c788 float4
  , c789 float4
  , c790 float4
  , c791 float4
  , c792 float4
  , c793 float4
  , c794 float4
  , c795 float4
  , c796 float4
  , c797 float4
  , c798 float4
  , c799 float4
  , c800 float4
  , c801 float4
  , c802 float4
  , c803 float4
  , c804 float4
  , c805 float4
  , c806 float4
  , c807 float4
  , c808 float4
  , c809 float4
  , c810 float4
  , c811 float4
  , c812 float4
  , c813 float4
  , c814 float4
  , c815 float4
  , c816 float4
  , c817 float4
  , c818 float4
  , c819 float4
  , c820 float4
  , c821 float4
  , c822 float4
  , c823 float4
  , c824 float4
  , c825 float4
  , c826 float4
  , c827 float4
  , c828 float4
  , c829 float4
  , c830 float4
  , c831 float4
  , c832 float4
  , c833 float4
  , c834 float4
  , c835 float4
  , c836 float4
  , c837 float4
  , c838 float4
  , c839 float4
  , c840 float4
  , c841 float4
  , c842 float4
  , c843 float4
  , c844 float4
  , c845 float4
  , c846 float4
  , c847 float4
  , c848 float4
  , c849 float4
  , c850 float4
  , c851 float4
  , c852 float4
  , c853 float4
  , c854 float4
  , c855 float4
  , c856 float4
  , c857 float4
  , c858 float4
  , c859 float4
  , c860 float4
  , c861 float4
  , c862 float4
  , c863 float4
  , c864 float4
  , c865 float4
  , c866 float4
  , c867 float4
  , c868 float4
  , c869 float4
  , c870 float4
  , c871 float4
  , c872 float4
  , c873 float4
  , c874 float4
  , c875 float4
  , c876 float4
  , c877 float4
  , c878 float4
  , c879 float4
  , c880 float4
  , c881 float4
  , c882 float4
  , c883 float4
  , c884 float4
  , c885 float4
  , c886 float4
  , c887 float4
  , c888 float4
  , c889 float4
  , c890 float4
  , c891 float4
  , c892 float4
  , c893 float4
  , c894 float4
  , c895 float4
  , c896 float4
  , c897 float4
  , c898 float4
  , c899 float4
  , c900 float4
  , c901 float4
  , c902 float4
  , c903 float4
  , c904 float4
  , c905 float4
  , c906 float4
  , c907 float4
  , c908 float4
  , c909 float4
  , c910 float4
  , c911 float4
  , c912 float4
  , c913 float4
  , c914 float4
  , c915 float4
  , c916 float4
  , c917 float4
  , c918 float4
  , c919 float4
  , c920 float4
  , c921 float4
  , c922 float4
  , c923 float4
  , c924 float4
  , c925 float4
  , c926 float4
  , c927 float4
  , c928 float4
  , c929 float4
  , c930 float4
  , c931 float4
  , c932 float4
  , c933 float4
  , c934 float4
  , c935 float4
  , c936 float4
  , c937 float4
  , c938 float4
  , c939 float4
  , c940 float4
  , c941 float4
  , c942 float4
  , c943 float4
  , c944 float4
  , c945 float4
  , c946 float4
  , c947 float4
  , c948 float4
  , c949 float4
  , c950 float4
  , c951 float4
  , c952 float4
  , c953 float4
  , c954 float4
  , c955 float4
  , c956 float4
  , c957 float4
  , c958 float4
  , c959 float4
  , c960 float4
  , c961 float4
  , c962 float4
  , c963 float4
  , c964 float4
  , c965 float4
  , c966 float4
  , c967 float4
  , c968 float4
  , c969 float4
  , c970 float4
  , c971 float4
  , c972 float4
  , c973 float4
  , c974 float4
  , c975 float4
  , c976 float4
  , c977 float4
  , c978 float4
  , c979 float4
  , c980 float4
  , c981 float4
  , c982 float4
  , c983 float4
  , c984 float4
  , c985 float4
  , c986 float4
  , c987 float4
  , c988 float4
  , c989 float4
  , c990 float4
  , c991 float4
  , c992 float4
  , c993 float4
  , c994 float4
  , c995 float4
  , c996 float4
  , ext json
)
USING mars2 WITH ( compress_threshold='1000', chunk_size='32' )
DISTRIBUTED BY (vin)
PARTITION BY RANGE(ts) (
	START ('2015-12-31 00:00:00')
	END ('2016-01-02 00:00:00')
	EVERY ('86400 second'),
	DEFAULT PARTITION default_prt
);

CREATE INDEX IF NOT EXISTS "idx_xx" ON "public"."xx"
USING mars2_btree(
	vin
  , ts
)
WITH(uniquemode=false);
`))
	})
})
var _ = Describe("Metadata test", func() {
	Describe("GetRandomVinsGenerator", func() {
		var (
			meta      *Metadata
			generator func() string
		)

		It("returns a comma-separated list of 3 VINs", func() {
			// Create a new Metadata object with some test data
			meta = &Metadata{
				Table: &Table{
					VinValues: []string{"VIN1", "VIN2", "VIN3", "VIN4", "VIN5"},
				},
			}

			// Call the GetRandomVinsGenerator function with num=3
			generator = meta.GetRandomVinsGenerator(3)

			result := generator()

			Expect(strings.Count(result, ",")).To(Equal(2))
		})

		It("returns an empty string when VinValues is empty", func() {
			// Create a new Metadata object with some test data
			meta = &Metadata{
				Table: &Table{
					VinValues: []string{""},
				},
			}

			generator = meta.GetRandomVinsGenerator(1)

			result := generator()

			Expect(result).To(Equal("''"))
		})
	})
})
