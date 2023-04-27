package telematics

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
)

var _ = Describe("Single Tag Latest Query", func() {
	It("should generate SQL without dealing with json column", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		b := Benchmark{}
		b.gcfg.GlobalCfg = engine.GlobalConfig{
			SchemaName:            "public",
			TableName:             "xx",
			TotalMetricsCount:     20,
			TimestampStepInSecond: 1,
			MetricsType:           metadata.MetricsTypeFloat4,
			StartAt:               startAt,
			EndAt:                 endAt,
			TagNum:                25000,
		}

		b.meta, _ = metadata.New(b.gcfg.GlobalCfg.NewMetadataConfig())
		q := newQuerySingleLatest(b.meta, nil)
		query, ok := q.(*querySingleLatest)
		Expect(ok).To(BeTrue())
		query.singleVinGenerator = func() string {
			return "'test'"
		}

		Expect(query.GetSQL()).To(Equal(`SELECT
    ts
  , vin
  , c0
  , c1
  , c2
  , c3
  , c4
  , c5
  , c6
  , c7
  , c8
  , c9
  , c10
  , c11
  , c12
  , c13
  , c14
  , c15
  , c16
  , c17
  , c18
  , c19
FROM "public"."xx"
WHERE vin = 'test'
ORDER BY ts DESC LIMIT 1`))
	})
	It("should generate SQL without dealing with json column and singleVinGenerator is empty", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		b := Benchmark{}
		b.gcfg.GlobalCfg = engine.GlobalConfig{
			SchemaName:            "public",
			TableName:             "xx",
			TotalMetricsCount:     20,
			TimestampStepInSecond: 1,
			MetricsType:           metadata.MetricsTypeFloat4,
			StartAt:               startAt,
			EndAt:                 endAt,
			TagNum:                25000,
		}

		b.meta, _ = metadata.New(b.gcfg.GlobalCfg.NewMetadataConfig())
		q := newQuerySingleLatest(b.meta, nil)
		query, ok := q.(*querySingleLatest)
		Expect(ok).To(BeTrue())
		query.singleVinGenerator = func() string {
			return "''"
		}

		Expect(query.GetSQL()).To(Equal(`SELECT
    ts
  , vin
  , c0
  , c1
  , c2
  , c3
  , c4
  , c5
  , c6
  , c7
  , c8
  , c9
  , c10
  , c11
  , c12
  , c13
  , c14
  , c15
  , c16
  , c17
  , c18
  , c19
FROM "public"."xx"
WHERE vin = ''
ORDER BY ts DESC LIMIT 1`))
	})
	It("should generate SQL with dealing with json column", func() {
		startAt, _ := time.Parse(util.TIME_FMT, "2016-01-01 00:00:00")
		endAt, _ := time.Parse(util.TIME_FMT, "2016-01-02 00:00:00")
		b := Benchmark{}
		b.gcfg.GlobalCfg = engine.GlobalConfig{
			SchemaName:            "public",
			TableName:             "xx",
			TotalMetricsCount:     999,
			TimestampStepInSecond: 1,
			MetricsType:           metadata.MetricsTypeFloat4,
			StartAt:               startAt,
			EndAt:                 endAt,
			TagNum:                25000,
		}

		b.meta, _ = metadata.New(b.gcfg.GlobalCfg.NewMetadataConfig())
		q := newQuerySingleLatest(b.meta, nil)
		query, ok := q.(*querySingleLatest)
		Expect(ok).To(BeTrue())
		query.singleVinGenerator = func() string {
			return "'test'"
		}

		Expect(query.GetSQL()).To(Equal(`SELECT
    t1.c0
  , t1.c1
  , t1.c2
  , t1.c3
  , t1.c4
  , t1.c5
  , t1.c6
  , t1.c7
  , t1.c8
  , t1.c9
  , t1.c10
  , t1.c11
  , t1.c12
  , t1.c13
  , t1.c14
  , t1.c15
  , t1.c16
  , t1.c17
  , t1.c18
  , t1.c19
  , t1.c20
  , t1.c21
  , t1.c22
  , t1.c23
  , t1.c24
  , t1.c25
  , t1.c26
  , t1.c27
  , t1.c28
  , t1.c29
  , t1.c30
  , t1.c31
  , t1.c32
  , t1.c33
  , t1.c34
  , t1.c35
  , t1.c36
  , t1.c37
  , t1.c38
  , t1.c39
  , t1.c40
  , t1.c41
  , t1.c42
  , t1.c43
  , t1.c44
  , t1.c45
  , t1.c46
  , t1.c47
  , t1.c48
  , t1.c49
  , t1.c50
  , t1.c51
  , t1.c52
  , t1.c53
  , t1.c54
  , t1.c55
  , t1.c56
  , t1.c57
  , t1.c58
  , t1.c59
  , t1.c60
  , t1.c61
  , t1.c62
  , t1.c63
  , t1.c64
  , t1.c65
  , t1.c66
  , t1.c67
  , t1.c68
  , t1.c69
  , t1.c70
  , t1.c71
  , t1.c72
  , t1.c73
  , t1.c74
  , t1.c75
  , t1.c76
  , t1.c77
  , t1.c78
  , t1.c79
  , t1.c80
  , t1.c81
  , t1.c82
  , t1.c83
  , t1.c84
  , t1.c85
  , t1.c86
  , t1.c87
  , t1.c88
  , t1.c89
  , t1.c90
  , t1.c91
  , t1.c92
  , t1.c93
  , t1.c94
  , t1.c95
  , t1.c96
  , t1.c97
  , t1.c98
  , t1.c99
  , t1.c100
  , t1.c101
  , t1.c102
  , t1.c103
  , t1.c104
  , t1.c105
  , t1.c106
  , t1.c107
  , t1.c108
  , t1.c109
  , t1.c110
  , t1.c111
  , t1.c112
  , t1.c113
  , t1.c114
  , t1.c115
  , t1.c116
  , t1.c117
  , t1.c118
  , t1.c119
  , t1.c120
  , t1.c121
  , t1.c122
  , t1.c123
  , t1.c124
  , t1.c125
  , t1.c126
  , t1.c127
  , t1.c128
  , t1.c129
  , t1.c130
  , t1.c131
  , t1.c132
  , t1.c133
  , t1.c134
  , t1.c135
  , t1.c136
  , t1.c137
  , t1.c138
  , t1.c139
  , t1.c140
  , t1.c141
  , t1.c142
  , t1.c143
  , t1.c144
  , t1.c145
  , t1.c146
  , t1.c147
  , t1.c148
  , t1.c149
  , t1.c150
  , t1.c151
  , t1.c152
  , t1.c153
  , t1.c154
  , t1.c155
  , t1.c156
  , t1.c157
  , t1.c158
  , t1.c159
  , t1.c160
  , t1.c161
  , t1.c162
  , t1.c163
  , t1.c164
  , t1.c165
  , t1.c166
  , t1.c167
  , t1.c168
  , t1.c169
  , t1.c170
  , t1.c171
  , t1.c172
  , t1.c173
  , t1.c174
  , t1.c175
  , t1.c176
  , t1.c177
  , t1.c178
  , t1.c179
  , t1.c180
  , t1.c181
  , t1.c182
  , t1.c183
  , t1.c184
  , t1.c185
  , t1.c186
  , t1.c187
  , t1.c188
  , t1.c189
  , t1.c190
  , t1.c191
  , t1.c192
  , t1.c193
  , t1.c194
  , t1.c195
  , t1.c196
  , t1.c197
  , t1.c198
  , t1.c199
  , t1.c200
  , t1.c201
  , t1.c202
  , t1.c203
  , t1.c204
  , t1.c205
  , t1.c206
  , t1.c207
  , t1.c208
  , t1.c209
  , t1.c210
  , t1.c211
  , t1.c212
  , t1.c213
  , t1.c214
  , t1.c215
  , t1.c216
  , t1.c217
  , t1.c218
  , t1.c219
  , t1.c220
  , t1.c221
  , t1.c222
  , t1.c223
  , t1.c224
  , t1.c225
  , t1.c226
  , t1.c227
  , t1.c228
  , t1.c229
  , t1.c230
  , t1.c231
  , t1.c232
  , t1.c233
  , t1.c234
  , t1.c235
  , t1.c236
  , t1.c237
  , t1.c238
  , t1.c239
  , t1.c240
  , t1.c241
  , t1.c242
  , t1.c243
  , t1.c244
  , t1.c245
  , t1.c246
  , t1.c247
  , t1.c248
  , t1.c249
  , t1.c250
  , t1.c251
  , t1.c252
  , t1.c253
  , t1.c254
  , t1.c255
  , t1.c256
  , t1.c257
  , t1.c258
  , t1.c259
  , t1.c260
  , t1.c261
  , t1.c262
  , t1.c263
  , t1.c264
  , t1.c265
  , t1.c266
  , t1.c267
  , t1.c268
  , t1.c269
  , t1.c270
  , t1.c271
  , t1.c272
  , t1.c273
  , t1.c274
  , t1.c275
  , t1.c276
  , t1.c277
  , t1.c278
  , t1.c279
  , t1.c280
  , t1.c281
  , t1.c282
  , t1.c283
  , t1.c284
  , t1.c285
  , t1.c286
  , t1.c287
  , t1.c288
  , t1.c289
  , t1.c290
  , t1.c291
  , t1.c292
  , t1.c293
  , t1.c294
  , t1.c295
  , t1.c296
  , t1.c297
  , t1.c298
  , t1.c299
  , t1.c300
  , t1.c301
  , t1.c302
  , t1.c303
  , t1.c304
  , t1.c305
  , t1.c306
  , t1.c307
  , t1.c308
  , t1.c309
  , t1.c310
  , t1.c311
  , t1.c312
  , t1.c313
  , t1.c314
  , t1.c315
  , t1.c316
  , t1.c317
  , t1.c318
  , t1.c319
  , t1.c320
  , t1.c321
  , t1.c322
  , t1.c323
  , t1.c324
  , t1.c325
  , t1.c326
  , t1.c327
  , t1.c328
  , t1.c329
  , t1.c330
  , t1.c331
  , t1.c332
  , t1.c333
  , t1.c334
  , t1.c335
  , t1.c336
  , t1.c337
  , t1.c338
  , t1.c339
  , t1.c340
  , t1.c341
  , t1.c342
  , t1.c343
  , t1.c344
  , t1.c345
  , t1.c346
  , t1.c347
  , t1.c348
  , t1.c349
  , t1.c350
  , t1.c351
  , t1.c352
  , t1.c353
  , t1.c354
  , t1.c355
  , t1.c356
  , t1.c357
  , t1.c358
  , t1.c359
  , t1.c360
  , t1.c361
  , t1.c362
  , t1.c363
  , t1.c364
  , t1.c365
  , t1.c366
  , t1.c367
  , t1.c368
  , t1.c369
  , t1.c370
  , t1.c371
  , t1.c372
  , t1.c373
  , t1.c374
  , t1.c375
  , t1.c376
  , t1.c377
  , t1.c378
  , t1.c379
  , t1.c380
  , t1.c381
  , t1.c382
  , t1.c383
  , t1.c384
  , t1.c385
  , t1.c386
  , t1.c387
  , t1.c388
  , t1.c389
  , t1.c390
  , t1.c391
  , t1.c392
  , t1.c393
  , t1.c394
  , t1.c395
  , t1.c396
  , t1.c397
  , t1.c398
  , t1.c399
  , t1.c400
  , t1.c401
  , t1.c402
  , t1.c403
  , t1.c404
  , t1.c405
  , t1.c406
  , t1.c407
  , t1.c408
  , t1.c409
  , t1.c410
  , t1.c411
  , t1.c412
  , t1.c413
  , t1.c414
  , t1.c415
  , t1.c416
  , t1.c417
  , t1.c418
  , t1.c419
  , t1.c420
  , t1.c421
  , t1.c422
  , t1.c423
  , t1.c424
  , t1.c425
  , t1.c426
  , t1.c427
  , t1.c428
  , t1.c429
  , t1.c430
  , t1.c431
  , t1.c432
  , t1.c433
  , t1.c434
  , t1.c435
  , t1.c436
  , t1.c437
  , t1.c438
  , t1.c439
  , t1.c440
  , t1.c441
  , t1.c442
  , t1.c443
  , t1.c444
  , t1.c445
  , t1.c446
  , t1.c447
  , t1.c448
  , t1.c449
  , t1.c450
  , t1.c451
  , t1.c452
  , t1.c453
  , t1.c454
  , t1.c455
  , t1.c456
  , t1.c457
  , t1.c458
  , t1.c459
  , t1.c460
  , t1.c461
  , t1.c462
  , t1.c463
  , t1.c464
  , t1.c465
  , t1.c466
  , t1.c467
  , t1.c468
  , t1.c469
  , t1.c470
  , t1.c471
  , t1.c472
  , t1.c473
  , t1.c474
  , t1.c475
  , t1.c476
  , t1.c477
  , t1.c478
  , t1.c479
  , t1.c480
  , t1.c481
  , t1.c482
  , t1.c483
  , t1.c484
  , t1.c485
  , t1.c486
  , t1.c487
  , t1.c488
  , t1.c489
  , t1.c490
  , t1.c491
  , t1.c492
  , t1.c493
  , t1.c494
  , t1.c495
  , t1.c496
  , t1.c497
  , t1.c498
  , t1.c499
  , t1.c500
  , t1.c501
  , t1.c502
  , t1.c503
  , t1.c504
  , t1.c505
  , t1.c506
  , t1.c507
  , t1.c508
  , t1.c509
  , t1.c510
  , t1.c511
  , t1.c512
  , t1.c513
  , t1.c514
  , t1.c515
  , t1.c516
  , t1.c517
  , t1.c518
  , t1.c519
  , t1.c520
  , t1.c521
  , t1.c522
  , t1.c523
  , t1.c524
  , t1.c525
  , t1.c526
  , t1.c527
  , t1.c528
  , t1.c529
  , t1.c530
  , t1.c531
  , t1.c532
  , t1.c533
  , t1.c534
  , t1.c535
  , t1.c536
  , t1.c537
  , t1.c538
  , t1.c539
  , t1.c540
  , t1.c541
  , t1.c542
  , t1.c543
  , t1.c544
  , t1.c545
  , t1.c546
  , t1.c547
  , t1.c548
  , t1.c549
  , t1.c550
  , t1.c551
  , t1.c552
  , t1.c553
  , t1.c554
  , t1.c555
  , t1.c556
  , t1.c557
  , t1.c558
  , t1.c559
  , t1.c560
  , t1.c561
  , t1.c562
  , t1.c563
  , t1.c564
  , t1.c565
  , t1.c566
  , t1.c567
  , t1.c568
  , t1.c569
  , t1.c570
  , t1.c571
  , t1.c572
  , t1.c573
  , t1.c574
  , t1.c575
  , t1.c576
  , t1.c577
  , t1.c578
  , t1.c579
  , t1.c580
  , t1.c581
  , t1.c582
  , t1.c583
  , t1.c584
  , t1.c585
  , t1.c586
  , t1.c587
  , t1.c588
  , t1.c589
  , t1.c590
  , t1.c591
  , t1.c592
  , t1.c593
  , t1.c594
  , t1.c595
  , t1.c596
  , t1.c597
  , t1.c598
  , t1.c599
  , t1.c600
  , t1.c601
  , t1.c602
  , t1.c603
  , t1.c604
  , t1.c605
  , t1.c606
  , t1.c607
  , t1.c608
  , t1.c609
  , t1.c610
  , t1.c611
  , t1.c612
  , t1.c613
  , t1.c614
  , t1.c615
  , t1.c616
  , t1.c617
  , t1.c618
  , t1.c619
  , t1.c620
  , t1.c621
  , t1.c622
  , t1.c623
  , t1.c624
  , t1.c625
  , t1.c626
  , t1.c627
  , t1.c628
  , t1.c629
  , t1.c630
  , t1.c631
  , t1.c632
  , t1.c633
  , t1.c634
  , t1.c635
  , t1.c636
  , t1.c637
  , t1.c638
  , t1.c639
  , t1.c640
  , t1.c641
  , t1.c642
  , t1.c643
  , t1.c644
  , t1.c645
  , t1.c646
  , t1.c647
  , t1.c648
  , t1.c649
  , t1.c650
  , t1.c651
  , t1.c652
  , t1.c653
  , t1.c654
  , t1.c655
  , t1.c656
  , t1.c657
  , t1.c658
  , t1.c659
  , t1.c660
  , t1.c661
  , t1.c662
  , t1.c663
  , t1.c664
  , t1.c665
  , t1.c666
  , t1.c667
  , t1.c668
  , t1.c669
  , t1.c670
  , t1.c671
  , t1.c672
  , t1.c673
  , t1.c674
  , t1.c675
  , t1.c676
  , t1.c677
  , t1.c678
  , t1.c679
  , t1.c680
  , t1.c681
  , t1.c682
  , t1.c683
  , t1.c684
  , t1.c685
  , t1.c686
  , t1.c687
  , t1.c688
  , t1.c689
  , t1.c690
  , t1.c691
  , t1.c692
  , t1.c693
  , t1.c694
  , t1.c695
  , t1.c696
  , t1.c697
  , t1.c698
  , t1.c699
  , t1.c700
  , t1.c701
  , t1.c702
  , t1.c703
  , t1.c704
  , t1.c705
  , t1.c706
  , t1.c707
  , t1.c708
  , t1.c709
  , t1.c710
  , t1.c711
  , t1.c712
  , t1.c713
  , t1.c714
  , t1.c715
  , t1.c716
  , t1.c717
  , t1.c718
  , t1.c719
  , t1.c720
  , t1.c721
  , t1.c722
  , t1.c723
  , t1.c724
  , t1.c725
  , t1.c726
  , t1.c727
  , t1.c728
  , t1.c729
  , t1.c730
  , t1.c731
  , t1.c732
  , t1.c733
  , t1.c734
  , t1.c735
  , t1.c736
  , t1.c737
  , t1.c738
  , t1.c739
  , t1.c740
  , t1.c741
  , t1.c742
  , t1.c743
  , t1.c744
  , t1.c745
  , t1.c746
  , t1.c747
  , t1.c748
  , t1.c749
  , t1.c750
  , t1.c751
  , t1.c752
  , t1.c753
  , t1.c754
  , t1.c755
  , t1.c756
  , t1.c757
  , t1.c758
  , t1.c759
  , t1.c760
  , t1.c761
  , t1.c762
  , t1.c763
  , t1.c764
  , t1.c765
  , t1.c766
  , t1.c767
  , t1.c768
  , t1.c769
  , t1.c770
  , t1.c771
  , t1.c772
  , t1.c773
  , t1.c774
  , t1.c775
  , t1.c776
  , t1.c777
  , t1.c778
  , t1.c779
  , t1.c780
  , t1.c781
  , t1.c782
  , t1.c783
  , t1.c784
  , t1.c785
  , t1.c786
  , t1.c787
  , t1.c788
  , t1.c789
  , t1.c790
  , t1.c791
  , t1.c792
  , t1.c793
  , t1.c794
  , t1.c795
  , t1.c796
  , t1.c797
  , t1.c798
  , t1.c799
  , t1.c800
  , t1.c801
  , t1.c802
  , t1.c803
  , t1.c804
  , t1.c805
  , t1.c806
  , t1.c807
  , t1.c808
  , t1.c809
  , t1.c810
  , t1.c811
  , t1.c812
  , t1.c813
  , t1.c814
  , t1.c815
  , t1.c816
  , t1.c817
  , t1.c818
  , t1.c819
  , t1.c820
  , t1.c821
  , t1.c822
  , t1.c823
  , t1.c824
  , t1.c825
  , t1.c826
  , t1.c827
  , t1.c828
  , t1.c829
  , t1.c830
  , t1.c831
  , t1.c832
  , t1.c833
  , t1.c834
  , t1.c835
  , t1.c836
  , t1.c837
  , t1.c838
  , t1.c839
  , t1.c840
  , t1.c841
  , t1.c842
  , t1.c843
  , t1.c844
  , t1.c845
  , t1.c846
  , t1.c847
  , t1.c848
  , t1.c849
  , t1.c850
  , t1.c851
  , t1.c852
  , t1.c853
  , t1.c854
  , t1.c855
  , t1.c856
  , t1.c857
  , t1.c858
  , t1.c859
  , t1.c860
  , t1.c861
  , t1.c862
  , t1.c863
  , t1.c864
  , t1.c865
  , t1.c866
  , t1.c867
  , t1.c868
  , t1.c869
  , t1.c870
  , t1.c871
  , t1.c872
  , t1.c873
  , t1.c874
  , t1.c875
  , t1.c876
  , t1.c877
  , t1.c878
  , t1.c879
  , t1.c880
  , t1.c881
  , t1.c882
  , t1.c883
  , t1.c884
  , t1.c885
  , t1.c886
  , t1.c887
  , t1.c888
  , t1.c889
  , t1.c890
  , t1.c891
  , t1.c892
  , t1.c893
  , t1.c894
  , t1.c895
  , t1.c896
  , t1.c897
  , t1.c898
  , t1.c899
  , t1.c900
  , t1.c901
  , t1.c902
  , t1.c903
  , t1.c904
  , t1.c905
  , t1.c906
  , t1.c907
  , t1.c908
  , t1.c909
  , t1.c910
  , t1.c911
  , t1.c912
  , t1.c913
  , t1.c914
  , t1.c915
  , t1.c916
  , t1.c917
  , t1.c918
  , t1.c919
  , t1.c920
  , t1.c921
  , t1.c922
  , t1.c923
  , t1.c924
  , t1.c925
  , t1.c926
  , t1.c927
  , t1.c928
  , t1.c929
  , t1.c930
  , t1.c931
  , t1.c932
  , t1.c933
  , t1.c934
  , t1.c935
  , t1.c936
  , t1.c937
  , t1.c938
  , t1.c939
  , t1.c940
  , t1.c941
  , t1.c942
  , t1.c943
  , t1.c944
  , t1.c945
  , t1.c946
  , t1.c947
  , t1.c948
  , t1.c949
  , t1.c950
  , t1.c951
  , t1.c952
  , t1.c953
  , t1.c954
  , t1.c955
  , t1.c956
  , t1.c957
  , t1.c958
  , t1.c959
  , t1.c960
  , t1.c961
  , t1.c962
  , t1.c963
  , t1.c964
  , t1.c965
  , t1.c966
  , t1.c967
  , t1.c968
  , t1.c969
  , t1.c970
  , t1.c971
  , t1.c972
  , t1.c973
  , t1.c974
  , t1.c975
  , t1.c976
  , t1.c977
  , t1.c978
  , t1.c979
  , t1.c980
  , t1.c981
  , t1.c982
  , t1.c983
  , t1.c984
  , t1.c985
  , t1.c986
  , t1.c987
  , t1.c988
  , t1.c989
  , t1.c990
  , t1.c991
  , t1.c992
  , t1.c993
  , t1.c994
  , t1.c995
  , t1.c996
  , t2.k0
  , t2.k1
FROM "public"."xx" AS t1
, json_to_record(t1.ext) AS t2 ( "k0" float4
  , "k1" float4 )
WHERE vin = 'test'
ORDER BY ts DESC LIMIT 1`))
	})

})
