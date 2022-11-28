package mxmock

import (
	"fmt"
	"strings"

	"github.com/brianvoe/gofakeit/v6"
)

type Box struct {
	BaseType
}

func NewBox(colName string) Type {
	return &Box{
		BaseType: NewBaseType(colName),
	}
}

func (b *Box) Random(keys ...string) string {
	for _, key := range keys {
		if key != b.colName {
			continue
		}
		return fmt.Sprintf(
			"%d,%d,%d,%d",
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
		)
	}
	return ""
}

type Boxes struct {
	BaseType
	boxes []*Box
}

func NewBoxes(colName string) Type {
	return &Boxes{
		BaseType: NewBaseType(colName),
		boxes:    []*Box{NewBox(colName).(*Box)},
	}
}

func (bs Boxes) Random(keys ...string) string {
	for _, key := range keys {
		if key != bs.colName {
			continue
		}
		a := []string{}
		for _, b := range bs.boxes {
			a = append(a, b.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (bs Boxes) Parse(string) {}

type Circle struct {
	BaseType
}

func NewCircle(colName string) Type {
	return &Circle{
		BaseType: NewBaseType(colName),
	}
}

func (c *Circle) Random(keys ...string) string {
	for _, key := range keys {
		if key != c.colName {
			continue
		}
		return fmt.Sprintf(
			"<(%d,%d),%d>",
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
		)
	}
	return ""
}

type Circles struct {
	BaseType
	circles []*Circle
}

func NewCircles(colName string) Type {
	return &Circles{
		BaseType: NewBaseType(colName),
		circles:  []*Circle{NewCircle(colName).(*Circle)},
	}
}

func (cs Circles) Random(keys ...string) string {
	for _, key := range keys {
		if key != cs.colName {
			continue
		}
		a := []string{}
		for _, c := range cs.circles {
			a = append(a, c.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (cs Circles) Parse(string) {}

type Line = Box

type Lines = Boxes

type LSeg = Box

type LSegs = Boxes

type Path = Box

type Paths = Boxes

type Polygon = Box

type Polygons = Boxes

type Point struct {
	BaseType
}

func NewPoint(colName string) Type {
	return &Point{
		BaseType: NewBaseType(colName),
	}
}

func (p *Point) Random(keys ...string) string {
	for _, key := range keys {
		if key != p.colName {
			continue
		}
		return fmt.Sprintf(
			"%d,%d",
			gofakeit.Number(1, 999),
			gofakeit.Number(1, 999),
		)
	}
	return ""
}

type Points struct {
	BaseType
	points []*Point
}

func NewPoints(colName string) Type {
	return &Points{
		BaseType: NewBaseType(colName),
		points:   []*Point{NewPoint(colName).(*Point)},
	}
}

func (ps Points) Random(keys ...string) string {
	for _, key := range keys {
		if key != ps.colName {
			continue
		}
		a := []string{}
		for _, p := range ps.points {
			a = append(a, p.Random(keys...))
		}
		return "{" + strings.Join(a, ",") + "}"
	}
	return "{}"
}

func (ps Points) Parse(string) {}
