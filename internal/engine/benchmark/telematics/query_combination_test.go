package telematics

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Combination query test", func() {
	Context("Test parseCombinationQueries", func() {
		Context("General tests", func() {
			It("should return a nil slice and no error with the input of an empty string", func() {
				emptyString := ""
				queries, err := parseCombinationQueries(emptyString)
				Expect(queries).To(BeNil())
				Expect(err).NotTo(HaveOccurred())

				emptyString2 := "                   "
				queries, err = parseCombinationQueries(emptyString2)
				Expect(queries).To(BeNil())
				Expect(err).NotTo(HaveOccurred())

				emptyString3 := `                   
				
				
				
				`
				queries, err = parseCombinationQueries(emptyString3)
				Expect(queries).To(BeNil())
				Expect(err).NotTo(HaveOccurred())
			})
			It("should return a non-empty query slice with length 0 and no error with the input of '[]'", func() {
				squareBracketString := "[]"
				queries, err := parseCombinationQueries(squareBracketString)
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(0))
				Expect(cap(queries)).To(Equal(0))
				Expect(err).NotTo(HaveOccurred())

				squareBracketString2 := "[        ]"
				queries, err = parseCombinationQueries(squareBracketString2)
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(0))
				Expect(cap(queries)).To(Equal(0))
				Expect(err).NotTo(HaveOccurred())

				squareBracketString3 := "    [        ]   "
				queries, err = parseCombinationQueries(squareBracketString3)
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(0))
				Expect(cap(queries)).To(Equal(0))
				Expect(err).NotTo(HaveOccurred())

				squareBracketString4 := `    [      
	
	
	
	
	
	
	
				  ]   `
				queries, err = parseCombinationQueries(squareBracketString4)
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(0))
				Expect(cap(queries)).To(Equal(0))
				Expect(err).NotTo(HaveOccurred())
			})
		})
		Context("test parsing projection expression", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.Projections).To(BeNil())
				Expect(query.Limit).To(Equal(0))
			})
			It("should return a slice with one query with non-nil projections expressions", func() {
				testString := `[
				{
				"projections": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.Projections).NotTo(BeNil())
				Expect(query.Projections.UseRawExpression).ToNot(BeTrue())
				Expect(query.Projections.Expression).To(Equal(""))
			})
			It("should return a slice with one query with non-nil projections expressions and with use-raw-expression true", func() {
				testString := `[
				{
				"projections": {
					"use-raw-expression": true,
					"expression": "*"
				}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.Projections).NotTo(BeNil())
				Expect(query.Projections.UseRawExpression).To(BeTrue())
				Expect(query.Projections.Expression).To(Equal("*"))
			})
		})
		Context("test parsing from expression", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.FromExpression).To(BeNil())
				Expect(query.Limit).To(Equal(0))
			})
			It("should return a slice with one query with non-nil from expression and with use-relation as false and nil statement", func() {
				testString := `[
				{
				"from": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.FromExpression).NotTo(BeNil())
				Expect(query.FromExpression.UseRawExpression).ToNot(BeTrue())
				Expect(query.FromExpression.Expression).To(Equal(""))

				Expect(query.FromExpression.UseRelationIdentifier).ToNot(BeTrue())
				Expect(query.FromExpression.RelationIdentifier).To(Equal(""))

				Expect(query.FromExpression.RelationStatement).To(BeNil())
			})
			It("should return a slice with one query with non-nil from expression and with use-relation as true", func() {
				testString := `[
				{
				"from": {"use-relation-identifier": true, "relation-identifier": "test_table"}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.FromExpression).NotTo(BeNil())
				Expect(query.FromExpression.UseRawExpression).ToNot(BeTrue())
				Expect(query.FromExpression.Expression).To(Equal(""))

				Expect(query.FromExpression.UseRelationIdentifier).To(BeTrue())
				Expect(query.FromExpression.RelationIdentifier).To(Equal("test_table"))

				Expect(query.FromExpression.RelationStatement).To(BeNil())
			})
			It("should return a slice with one query with non-nil from expression and with relation-statement as not nil", func() {
				testString := `[
				{
					"from": {
						"relation-statement": 
						{
							"projections": {"use-raw-expression": true, "expression": "*"},
							"from": {"use-relation-identifier": true, "relation-identifier": "test_sub_table"}
						}
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.FromExpression).NotTo(BeNil())
				Expect(query.FromExpression.UseRawExpression).ToNot(BeTrue())
				Expect(query.FromExpression.Expression).To(Equal(""))

				Expect(query.FromExpression.UseRelationIdentifier).NotTo(BeTrue())
				Expect(query.FromExpression.RelationIdentifier).To(Equal(""))

				Expect(query.FromExpression.RelationStatement).NotTo(BeNil())
				Expect(query.FromExpression.RelationStatement.Projections).NotTo(BeNil())
				Expect(query.FromExpression.RelationStatement.Projections.UseRawExpression).To(BeTrue())
				Expect(query.FromExpression.RelationStatement.Projections.Expression).To(Equal("*"))

				Expect(query.FromExpression.RelationStatement.FromExpression).NotTo(BeNil())
				Expect(query.FromExpression.RelationStatement.FromExpression.UseRelationIdentifier).To(BeTrue())
				Expect(query.FromExpression.RelationStatement.FromExpression.RelationIdentifier).To(Equal("test_sub_table"))
			})
		})
		Context("test parsing device id predicate", func() {
			It("should return a slice with one query with nil sub expressions", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.DevicePredicate).To(BeNil())
			})
			It("should return a slice with one query with non-nil device predicate", func() {
				testString := `[
				{
					"device-predicate": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.DevicePredicate).NotTo(BeNil())
				Expect(query.DevicePredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.DevicePredicate.Expression).To(Equal(""))
				Expect(query.DevicePredicate.IsRandom).NotTo(BeTrue())
				Expect(query.DevicePredicate.Count).To(Equal(0))
				Expect(query.DevicePredicate.HasAlias).NotTo(BeTrue())
				Expect(query.DevicePredicate.Alias).To(Equal(""))
			})
			It("should return a slice with one query with non-nil device predicate and with 19 random value", func() {
				testString := `[
				{
					"device-predicate": {
						"is-random": true,
						"count": 19
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.DevicePredicate).NotTo(BeNil())
				Expect(query.DevicePredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.DevicePredicate.IsRandom).To(BeTrue())
				Expect(query.DevicePredicate.Count).To(Equal(19))
			})
			It("should return a slice with one query with non-nil device predicate and with 1 random value and an alias", func() {
				testString := `[
				{
					"device-predicate": {
						"is-random": true,
						"count": 1,
						"has-alias": true,
						"alias": "yet-another-device-col-name"
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.DevicePredicate).NotTo(BeNil())
				Expect(query.DevicePredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.DevicePredicate.IsRandom).To(BeTrue())
				Expect(query.DevicePredicate.Count).To(Equal(1))
				Expect(query.DevicePredicate.HasAlias).To(BeTrue())
				Expect(query.DevicePredicate.Alias).To(Equal("yet-another-device-col-name"))
			})
		})
		Context("test parsing timestamp predicate", func() {
			It("should return a slice with one query with nil sub expressions", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.TimestampPredicate).To(BeNil())
			})
			It("should return a slice with one query with non-nil timestamp expression", func() {
				testString := `[
				{
					"ts-predicate": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.TimestampPredicate).NotTo(BeNil())
				Expect(query.TimestampPredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Expression).To(Equal(""))

				Expect(query.TimestampPredicate.IsRandom).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Duration).To(Equal(0))

				Expect(query.TimestampPredicate.HasAlias).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Alias).To(Equal(""))

				Expect(query.TimestampPredicate.StartTime.IsZero()).To(BeTrue())
				Expect(query.TimestampPredicate.EndTime.IsZero()).To(BeTrue())

				Expect(query.TimestampPredicate.StartExclusive).NotTo(BeTrue())
				Expect(query.TimestampPredicate.EndExclusive).NotTo(BeTrue())
			})
			It("should return a slice with one query with non-nil timestamp expression and random with duration of 3600", func() {
				testString := `[
				{
					"ts-predicate": {
						"is-random": true,
						"duration": 3600 
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.TimestampPredicate).NotTo(BeNil())
				Expect(query.TimestampPredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Expression).To(Equal(""))

				Expect(query.TimestampPredicate.IsRandom).To(BeTrue())
				Expect(query.TimestampPredicate.Duration).To(Equal(3600))

				Expect(query.TimestampPredicate.HasAlias).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Alias).To(Equal(""))

				Expect(query.TimestampPredicate.StartTime.IsZero()).To(BeTrue())
				Expect(query.TimestampPredicate.EndTime.IsZero()).To(BeTrue())

				Expect(query.TimestampPredicate.StartExclusive).NotTo(BeTrue())
				Expect(query.TimestampPredicate.EndExclusive).NotTo(BeTrue())
			})
			It("should return a slice with one query with non-nil timestamp expression and non-zero start and end non-exclusive", func() {
				testString := `[
				{
					"ts-predicate": {
						"start": "2022-07-19 17:00:01",
						"end": "2022-07-19 18:30:50"
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.TimestampPredicate).NotTo(BeNil())
				Expect(query.TimestampPredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Expression).To(Equal(""))

				Expect(query.TimestampPredicate.IsRandom).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Duration).To(Equal(0))

				Expect(query.TimestampPredicate.HasAlias).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Alias).To(Equal(""))

				Expect(query.TimestampPredicate.StartTime.IsZero()).NotTo(BeTrue())
				Expect(query.TimestampPredicate.StartTime.Day()).To(Equal(19))
				Expect(query.TimestampPredicate.StartTime.Hour()).To(Equal(17))

				Expect(query.TimestampPredicate.EndTime.IsZero()).NotTo(BeTrue())
				Expect(query.TimestampPredicate.EndTime.Hour()).To(Equal(18))
				Expect(query.TimestampPredicate.EndTime.Second()).To(Equal(50))

				Expect(query.TimestampPredicate.StartExclusive).NotTo(BeTrue())
				Expect(query.TimestampPredicate.EndExclusive).NotTo(BeTrue())
			})
			It("should return a slice with one query with non-nil timestamp expression and non-zero start non-exclusive and zero end", func() {
				testString := `[
				{
					"ts-predicate": {
						"start": "2022-07-19 17:00:01",
						"start-exclusive": true
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.TimestampPredicate).NotTo(BeNil())
				Expect(query.TimestampPredicate.UseRawExpression).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Expression).To(Equal(""))

				Expect(query.TimestampPredicate.IsRandom).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Duration).To(Equal(0))

				Expect(query.TimestampPredicate.HasAlias).NotTo(BeTrue())
				Expect(query.TimestampPredicate.Alias).To(Equal(""))

				Expect(query.TimestampPredicate.StartTime.IsZero()).NotTo(BeTrue())
				Expect(query.TimestampPredicate.EndTime.IsZero()).To(BeTrue())

				Expect(query.TimestampPredicate.StartExclusive).To(BeTrue())
				Expect(query.TimestampPredicate.EndExclusive).NotTo(BeTrue())
			})
			It("should return a slice with one query with error", func() {
				testString := `[
				{
					"ts-predicate": {
						"start": "2022-07-19 17s1",
						"end": "2022-07-19 18:30:50"
					}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).To(HaveOccurred())
				Expect(queries).To(BeNil())
			})
		})
		Context("test parsing metrics predicate", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.MetricsPredicate).To(BeNil())
				Expect(query.Limit).To(Equal(0))
			})
			It("should return a slice with one query with non-nil metrics predicate expression", func() {
				testString := `[
				{
				"metrics-predicate": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.MetricsPredicate).NotTo(BeNil())
				Expect(query.MetricsPredicate.UseRawExpression).ToNot(BeTrue())
				Expect(query.MetricsPredicate.Expression).To(Equal(""))
			})
			It("should return a slice with one query with non-nil metrics predicate expression and use raw expression", func() {
				testString := `[
				{
				"metrics-predicate": {
					"use-raw-expression": true,
					"expression": "m1>13.4"
				}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.MetricsPredicate).NotTo(BeNil())
				Expect(query.MetricsPredicate.UseRawExpression).To(BeTrue())
				Expect(query.MetricsPredicate.Expression).To(Equal("m1>13.4"))
			})
		})
		Context("test parsing group by expression", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.GroupByPredicate).To(BeNil())
				Expect(query.Limit).To(Equal(0))
			})
			It("should return a slice with one query with non-nil group by expression", func() {
				testString := `[
				{
				"group-by": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.GroupByPredicate).NotTo(BeNil())
				Expect(query.GroupByPredicate.UseRawExpression).ToNot(BeTrue())
				Expect(query.GroupByPredicate.Expression).To(Equal(""))
			})
			It("should return a slice with one query with non-nil group by expression and use raw expression", func() {
				testString := `[
				{
				"group-by": {
					"use-raw-expression": true,
					"expression": "group_key"
				}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.GroupByPredicate).NotTo(BeNil())
				Expect(query.GroupByPredicate.UseRawExpression).To(BeTrue())
				Expect(query.GroupByPredicate.Expression).To(Equal("group_key"))
			})
		})
		Context("test parsing order by expression", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.OrderByPredicate).To(BeNil())
				Expect(query.Limit).To(Equal(0))
			})
			It("should return a slice with one query with non-nil order by expression", func() {
				testString := `[
				{
				"order-by": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.OrderByPredicate).NotTo(BeNil())
				Expect(query.OrderByPredicate.UseRawExpression).ToNot(BeTrue())
				Expect(query.OrderByPredicate.Expression).To(Equal(""))
			})
			It("should return a slice with one query with non-nil order by expression and use raw expression", func() {
				testString := `[
				{
				"order-by": {
					"use-raw-expression": true,
					"expression": "order_key DESC"
				}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.OrderByPredicate).NotTo(BeNil())
				Expect(query.OrderByPredicate.UseRawExpression).To(BeTrue())
				Expect(query.OrderByPredicate.Expression).To(Equal("order_key DESC"))
			})
		})
		Context("test parsing limit expression", func() {
			It("should return a slice with one query with a limit of 1", func() {
				testString := `[
				{
					"limit": 12
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(err).NotTo(HaveOccurred())
				Expect(queries).NotTo(BeNil())
				Expect(len(queries)).To(Equal(1))

				query := queries[0]
				Expect(query.Limit).To(Equal(12))
			})
		})
		Context("test parsing multiple queries", func() {
			It("should return a slice with one query with nil sub expressions and a limit of 0", func() {
				testString := `[
				{
					"from": {
						"relation-statement": 
						{
							"projections": {"use-raw-expression": true, "expression": "*"},
							"from": {"use-relation-identifier": true, "relation-identifier": "test_sub_table"}
						}
					}
				},
				{
					"ts-predicate": {
						"start": "2022-07-19 17:00:01",
						"start-exclusive": true
					}
				},
				{
					"order-by": {}
				}
				]`
				queries, err := parseCombinationQueries(testString)
				Expect(queries).NotTo(BeNil())
				Expect(err).NotTo(HaveOccurred())

				Expect(len(queries)).To(Equal(3))
			})
		})
	})
})
