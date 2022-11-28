package telematics

import (
	"bytes"
	"fmt"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/ymatrix-data/mxbench/internal/engine"
	"github.com/ymatrix-data/mxbench/internal/engine/metadata"
	"github.com/ymatrix-data/mxbench/internal/util"
)

var _ = Describe("Telematics Generator", func() {
	Context("calTagIndexesOfBatches", func() {
		It("write-batch-size is a multiple of the size row(s) per ts per tag", func() {
			generator := NewGenerator(engine.GeneratorConfig{
				GlobalConfig: &engine.GlobalConfig{
					TagNum: 65536 * 2,
				},
				PluginConfig: &Config{
					WriteBatchSize: 1, // 1024 * 1024 bytes
				},
			}).(*Generator)
			batches := make([][]string, 0)
			for i := 0; i < 65536*2; i++ {
				batches = append(batches, []string{"1|2|||||", "||||3|4|"}) // 16 bytes per batch
			}
			// 65536 tags will be into 1 batch
			tagRanges := generator.calTagIndexesOfBatches(batches)
			Expect(len(tagRanges)).To(Equal(2))
			Expect(tagRanges[0]).To(Equal(int64(65536)))
			Expect(tagRanges[1]).To(Equal(int64(65536 * 2)))
		})
		It("write-batch-size is a multiple of the size row(s) per ts per tag, the last batch is not full", func() {
			generator := NewGenerator(engine.GeneratorConfig{
				GlobalConfig: &engine.GlobalConfig{
					TagNum: 65536*2 + 1,
				},
				PluginConfig: &Config{
					WriteBatchSize: 1, // 1024 * 1024 bytes
				},
			}).(*Generator)
			batches := make([][]string, 0)
			for i := 0; i < 65536*2+1; i++ {
				batches = append(batches, []string{"1|2|||||", "||||3|4|"}) // 16 bytes per batch
			}
			tagRanges := generator.calTagIndexesOfBatches(batches)
			Expect(len(tagRanges)).To(Equal(3))
			Expect(tagRanges[0]).To(Equal(int64(65536)))
			Expect(tagRanges[1]).To(Equal(int64(65536 * 2)))
			Expect(tagRanges[2]).To(Equal(int64(65536*2 + 1)))
		})
		It("write-batch-size is not a multiple of the size row(s) per ts per tag", func() {
			generator := NewGenerator(engine.GeneratorConfig{
				GlobalConfig: &engine.GlobalConfig{
					TagNum: 1024*1024/18 + 1 + 2, // 58254 = 1024 * 1024 / 18
				},
				PluginConfig: &Config{
					WriteBatchSize: 1, // 1024 * 1024 bytes
				},
			}).(*Generator)
			batches := make([][]string, 0)
			for i := 0; i < 1024*1024/18+1+2; i++ {
				batches = append(batches, []string{"1|2||||||", "||||3|4||"}) // 18 bytes per batch
			}
			// 65536 tags will be into 1 batch
			tagRanges := generator.calTagIndexesOfBatches(batches)
			Expect(len(tagRanges)).To(Equal(2))
			Expect(tagRanges[0]).To(Equal(int64((1024*1024/18 + 1))))
			Expect(tagRanges[1]).To(Equal(int64(1024*1024/18 + 1 + 2)))
		})
	})
	Context("generateBatch", func() {
		Context("only one goroutine", func() {
			It("no delay data, no upsert, same length tag id", func() {
				generator := NewGenerator(engine.GeneratorConfig{
					GlobalConfig: &engine.GlobalConfig{
						TagNum: 5,
					},
					PluginConfig: &Config{
						WriteBatchSize:    1,
						NumGoRoutine:      1,
						percentOfOutOrder: 0, // No delayed data
					},
				}).(*Generator)
				tsString := "2022-07-26 09:39:59"
				ts, _ := time.Parse(util.TIME_FMT, tsString)
				vins := []string{"11", "12", "13", "14", "15"}
				// 8 bytes for each
				batches := [][]string{
					{"1|1|||||"},
					{"1|2|||||"},
					{"1|3|||||"},
					{"1|4|||||"},
					{"1|5|||||"},
				}
				batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

				Expect(len(batchData)).To(Equal(1))
				Expect(len(batchDataLines)).To(Equal(1))
				Expect(len(batchDataSize)).To(Equal(1))

				Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|13|1|3|||||
2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|15|1|5|||||
`))
				Expect(batchDataLines[0]).To(Equal(int64(5)))
				// the length of ts string  + the length of delimiter + the length of vin string + \
				// the length of delimiter + the length of data + the length of a "new line"
				Expect(batchDataSize[0]).To(Equal(int64(5 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))

			})
			It("no delay data, with upsert, same length tag id", func() {
				generator := NewGenerator(engine.GeneratorConfig{
					GlobalConfig: &engine.GlobalConfig{
						TagNum: 5,
					},
					PluginConfig: &Config{
						WriteBatchSize:    1,
						NumGoRoutine:      1,
						percentOfOutOrder: 0, // No delayed data
					},
				}).(*Generator)
				tsString := "2022-07-26 09:39:59"
				ts, _ := time.Parse(util.TIME_FMT, tsString)
				vins := []string{"11", "12", "13", "14", "15"}
				// 16 bytes for each
				batches := [][]string{
					{"1|1|||||", "||3|4|||"},
					{"1|2|||||", "||3|4|||"},
					{"1|3|||||", "||3|4|||"},
					{"1|4|||||", "||3|4|||"},
					{"1|5|||||", "||3|4|||"},
				}
				batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

				Expect(len(batchData)).To(Equal(1))
				Expect(len(batchDataLines)).To(Equal(1))
				Expect(len(batchDataSize)).To(Equal(1))

				Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|11|||3|4|||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|12|||3|4|||
2022-07-26 09:39:59|13|1|3|||||
2022-07-26 09:39:59|13|||3|4|||
2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|14|||3|4|||
2022-07-26 09:39:59|15|1|5|||||
2022-07-26 09:39:59|15|||3|4|||
`))
				Expect(batchDataLines[0]).To(Equal(int64(10)))
				// the length of ts string  + the length of delimiter + the length of vin string + \
				// the length of delimiter + the length of data + the length of a "new line"
				Expect(batchDataSize[0]).To(Equal(int64(10 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))

			})
		})
		Context("multiple goroutine", func() {
			Context("batch of each goroutine are full", func() {
				It("no delay data, no upsert, same length tag id", func() {
					generator := NewGenerator(engine.GeneratorConfig{
						GlobalConfig: &engine.GlobalConfig{
							TagNum: 5,
						},
						PluginConfig: &Config{
							WriteBatchSize:    1,
							NumGoRoutine:      2,
							percentOfOutOrder: 0, // No delayed data
						},
					}).(*Generator)
					tsString := "2022-07-26 09:39:59"
					ts, _ := time.Parse(util.TIME_FMT, tsString)
					vins := []string{"11", "12", "13", "14", "15", "16"}
					// 8 bytes for each
					batches := [][]string{
						{"1|1|||||"},
						{"1|2|||||"},
						{"1|3|||||"},
						{"1|4|||||"},
						{"1|5|||||"},
						{"1|6|||||"},
					}
					batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

					Expect(len(batchData)).To(Equal(2))
					Expect(len(batchDataLines)).To(Equal(2))
					Expect(len(batchDataSize)).To(Equal(2))

					Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|13|1|3|||||
`))
					Expect(string(batchData[1])).To(Equal(`2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|15|1|5|||||
2022-07-26 09:39:59|16|1|6|||||
`))
					Expect(batchDataLines[0]).To(Equal(int64(3)))
					Expect(batchDataLines[1]).To(Equal(int64(3)))
					// the length of ts string  + the length of delimiter + the length of vin string + \
					// the length of delimiter + the length of data + the length of a "new line"
					Expect(batchDataSize[0]).To(Equal(int64(3 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
					Expect(batchDataSize[1]).To(Equal(int64(3 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))

				})
				It("no delay data, with upsert, same length tag id", func() {
					generator := NewGenerator(engine.GeneratorConfig{
						GlobalConfig: &engine.GlobalConfig{
							TagNum: 5,
						},
						PluginConfig: &Config{
							WriteBatchSize:    1,
							NumGoRoutine:      2,
							percentOfOutOrder: 0, // No delayed data
						},
					}).(*Generator)
					tsString := "2022-07-26 09:39:59"
					ts, _ := time.Parse(util.TIME_FMT, tsString)
					vins := []string{"11", "12", "13", "14", "15", "16"}
					// 16 bytes for each
					batches := [][]string{
						{"1|1|||||", "||3|4|||"},
						{"1|2|||||", "||3|4|||"},
						{"1|3|||||", "||3|4|||"},
						{"1|4|||||", "||3|4|||"},
						{"1|5|||||", "||3|4|||"},
						{"1|6|||||", "||3|4|||"},
					}
					batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

					Expect(len(batchData)).To(Equal(2))
					Expect(len(batchDataLines)).To(Equal(2))
					Expect(len(batchDataSize)).To(Equal(2))

					Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|11|||3|4|||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|12|||3|4|||
2022-07-26 09:39:59|13|1|3|||||
2022-07-26 09:39:59|13|||3|4|||
`))
					Expect(string(batchData[1])).To(Equal(`2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|14|||3|4|||
2022-07-26 09:39:59|15|1|5|||||
2022-07-26 09:39:59|15|||3|4|||
2022-07-26 09:39:59|16|1|6|||||
2022-07-26 09:39:59|16|||3|4|||
`))
					Expect(batchDataLines[0]).To(Equal(int64(6)))
					Expect(batchDataLines[1]).To(Equal(int64(6)))
					// the length of ts string  + the length of delimiter + the length of vin string + \
					// the length of delimiter + the length of data + the length of a "new line"
					Expect(batchDataSize[0]).To(Equal(int64(6 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
					Expect(batchDataSize[1]).To(Equal(int64(6 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
				})
			})
			Context("the last goroutine is not full", func() {
				It("no delay data, no upsert, same length tag id", func() {
					generator := NewGenerator(engine.GeneratorConfig{
						GlobalConfig: &engine.GlobalConfig{
							TagNum: 5,
						},
						PluginConfig: &Config{
							WriteBatchSize:    1,
							NumGoRoutine:      2,
							percentOfOutOrder: 0, // No delayed data
						},
					}).(*Generator)
					tsString := "2022-07-26 09:39:59"
					ts, _ := time.Parse(util.TIME_FMT, tsString)
					vins := []string{"11", "12", "13", "14", "15"}
					// 8 bytes for each
					batches := [][]string{
						{"1|1|||||"},
						{"1|2|||||"},
						{"1|3|||||"},
						{"1|4|||||"},
						{"1|5|||||"},
					}
					batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

					Expect(len(batchData)).To(Equal(2))
					Expect(len(batchDataLines)).To(Equal(2))
					Expect(len(batchDataSize)).To(Equal(2))

					Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|13|1|3|||||
`))
					Expect(string(batchData[1])).To(Equal(`2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|15|1|5|||||
`))
					Expect(batchDataLines[0]).To(Equal(int64(3)))
					Expect(batchDataLines[1]).To(Equal(int64(2)))
					// the length of ts string  + the length of delimiter + the length of vin string + \
					// the length of delimiter + the length of data + the length of a "new line"
					Expect(batchDataSize[0]).To(Equal(int64(3 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
					Expect(batchDataSize[1]).To(Equal(int64(2 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))

				})
				It("no delay data, with upsert, same length tag id", func() {
					generator := NewGenerator(engine.GeneratorConfig{
						GlobalConfig: &engine.GlobalConfig{
							TagNum: 5,
						},
						PluginConfig: &Config{
							WriteBatchSize:    1,
							NumGoRoutine:      2,
							percentOfOutOrder: 0, // No delayed data
						},
					}).(*Generator)
					tsString := "2022-07-26 09:39:59"
					ts, _ := time.Parse(util.TIME_FMT, tsString)
					vins := []string{"11", "12", "13", "14", "15"}
					// 16 bytes for each
					batches := [][]string{
						{"1|1|||||", "||3|4|||"},
						{"1|2|||||", "||3|4|||"},
						{"1|3|||||", "||3|4|||"},
						{"1|4|||||", "||3|4|||"},
						{"1|5|||||", "||3|4|||"},
					}
					batchData, batchDataLines, batchDataSize := generator.generateBatch(vins, ts, batches)

					Expect(len(batchData)).To(Equal(2))
					Expect(len(batchDataLines)).To(Equal(2))
					Expect(len(batchDataSize)).To(Equal(2))

					Expect(string(batchData[0])).To(Equal(`2022-07-26 09:39:59|11|1|1|||||
2022-07-26 09:39:59|11|||3|4|||
2022-07-26 09:39:59|12|1|2|||||
2022-07-26 09:39:59|12|||3|4|||
2022-07-26 09:39:59|13|1|3|||||
2022-07-26 09:39:59|13|||3|4|||
`))
					Expect(string(batchData[1])).To(Equal(`2022-07-26 09:39:59|14|1|4|||||
2022-07-26 09:39:59|14|||3|4|||
2022-07-26 09:39:59|15|1|5|||||
2022-07-26 09:39:59|15|||3|4|||
`))
					Expect(batchDataLines[0]).To(Equal(int64(6)))
					Expect(batchDataLines[1]).To(Equal(int64(4)))
					// the length of ts string  + the length of delimiter + the length of vin string + \
					// the length of delimiter + the length of data + the length of a "new line"
					Expect(batchDataSize[0]).To(Equal(int64(6 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
					Expect(batchDataSize[1]).To(Equal(int64(4 * (len(tsString) + 1 + len("11") + 1 + 8 + 1))))
				})
			})
		})
	})
	Context("generateAndWriteBatch", func() {
		tagNum := 65536 * 2
		vinValues := make([]string, 0)
		for idx := 0; idx < tagNum; idx++ {
			vinValues = append(vinValues, strconv.Itoa(idx))
		}

		It("send with one goroutine", func() {
			outPut := bytes.NewBuffer(make([]byte, 0, 1*_MEGA_BYTES))
			generator := NewGenerator(engine.GeneratorConfig{
				GlobalConfig: &engine.GlobalConfig{
					TagNum: int64(tagNum),
				},
				PluginConfig: &Config{
					WriteBatchSize: 1, // 1024 * 1024 bytes
					NumGoRoutine:   1,
					templateSize:   1,
				},
			}).(*Generator)
			generator.writeFunc = func(s []byte, i1, i2 int64) error {
				_, _ = fmt.Fprint(outPut, string(s))
				return nil
			}
			generator.meta = &metadata.Metadata{
				Table: &metadata.Table{
					VinValues: vinValues,
				},
			}
			// // 65536 tags will be into 1 batch
			// tagRanges := generator.calTagIndexesOfOneBatch(rows)
			// Expect(len(tagRanges)).To(Equal(2))
			// Expect(tagRanges[0]).To(Equal(int64(65536)))
			// Expect(tagRanges[1]).To(Equal(int64(65536 * 2)))
			tsString := "2022-07-26 09:39:59"
			ts, _ := time.Parse(util.TIME_FMT, tsString)
			batches := make([][]string, tagNum)
			tpl := [][]string{{"1|2|||||", "||3|4|||"}} // 16 bytes
			err := generator.generateAndWriteBatch(batches, tpl, ts)
			Expect(err).ToNot(HaveOccurred())
			// tags number: 65536 * 2
			// lines per tag: 2
			// size per line without tag id: len(tsString) + 1 + 1 + 8 + 1
			totalSize := tagNum * 2 * (len(tsString) + 1 + 1 + 8 + 1)
			for i := 0; i < tagNum; i++ {
				totalSize += 2 * len(strconv.Itoa(i))
			}
			Expect(outPut.Len()).To(Equal(totalSize))
		})
		It("send with multiple goroutines", func() {
			var length int
			outPutChan := make(chan []byte, 2)
			writeFinCh := make(chan struct{})
			generator := NewGenerator(engine.GeneratorConfig{
				GlobalConfig: &engine.GlobalConfig{
					TagNum: int64(tagNum),
				},
				PluginConfig: &Config{
					WriteBatchSize: 1, // 1024 * 1024 bytes
					NumGoRoutine:   2,
					templateSize:   1,
				},
			}).(*Generator)
			generator.writeFunc = func(s []byte, i1, i2 int64) error {
				outPutChan <- s
				return nil
			}
			generator.meta = &metadata.Metadata{
				Table: &metadata.Table{
					VinValues: vinValues,
				},
			}
			endChan := make(chan struct{})
			go func() {
				defer close(writeFinCh)
				for {
					select {
					case s, ok := <-outPutChan:
						if ok {
							length += len(s)
						}
					case <-endChan:
						// drain outPutChan
						for s := range outPutChan {
							length += len(s)
						}
						return
					}
				}
			}()
			// // 65536 tags will be into 1 batch
			// tagRanges := generator.calTagIndexesOfOneBatch(rows)
			// Expect(len(tagRanges)).To(Equal(2))
			// Expect(tagRanges[0]).To(Equal(int64(65536)))
			// Expect(tagRanges[1]).To(Equal(int64(65536 * 2)))
			tsString := "2022-07-26 09:39:59"
			ts, _ := time.Parse(util.TIME_FMT, tsString)
			batches := make([][]string, tagNum)
			tpl := [][]string{{"1|2|||||", "||3|4|||"}} // 16 bytes
			err := generator.generateAndWriteBatch(batches, tpl, ts)
			close(endChan)
			close(outPutChan)
			<-writeFinCh
			Expect(err).ToNot(HaveOccurred())
			// tags number: 65536 * 2
			// lines per tag: 2
			// size per line without tag id: len(tsString) + 1 + 1 + 8 + 1
			totalSize := tagNum * 2 * (len(tsString) + 1 + 1 + 8 + 1)
			for i := 0; i < tagNum; i++ {
				totalSize += 2 * len(strconv.Itoa(i))
			}
			Expect(length).To(Equal(totalSize))
		})
	})
})
